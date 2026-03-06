from typing import Optional, Union

from job_hive.queue.base import BaseQueue
from job_hive.core import Status
from job_hive.utils import as_string, get_now, safe_loads_with_compression
from job_hive.job import Job

try:
    import redis
except ImportError:
    raise ImportError('RedisQueue requires redis-py to be installed.')


class RedisQueue(BaseQueue):

    def __init__(
            self,
            name: str,
            host: str = "localhost",
            port: int = 6379,
            db: int = 0,
            password: str = None,
            socket_timeout: float = 30.0,
            socket_connect_timeout: float = 5.0,
            max_connections: int = 50,
    ):
        if name is None:
            raise ValueError('Queue name cannot be None.')
        self._queue_name = f"hive:queue:{name}"
        self._pool: redis.ConnectionPool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            max_connections=max_connections,
        )
        self._redis_client: Optional[redis.Redis] = None

    @property
    def conn(self) -> redis.Redis:
        """获取 Redis 连接（复用连接）"""
        if self._redis_client is None:
            self._redis_client = redis.Redis(connection_pool=self._pool)
        return self._redis_client

    def enqueue(self, *args: 'Job'):
        for job in args:
            job.query['created_at'] = job.created_at
            self.conn.hset(
                name=f"hive:job:{job.job_id}",
                mapping=job.dumps()
            )
        self.conn.rpush(
            self._queue_name,
            *(job.job_id for job in args)
        )

    def remove(self, job: 'Job'):
        self.conn.hdel(
            name=f"hive:job:{job.job_id}"
        )
        self.conn.lrem(
            self._queue_name,
            0,
            job.job_id
        )
        job.query.clear()

    def clear(self):
        """清空队列和所有相关任务数据"""
        # 获取队列中的所有任务ID
        job_ids = self.conn.lrange(self._queue_name, 0, -1)
        
        if job_ids:
            pipe = self.conn.pipeline()
            # 删除所有任务的 Hash 数据
            for job_id in job_ids:
                pipe.hdel(f"hive:job:{as_string(job_id)}")
            # 清空队列
            pipe.delete(self._queue_name)
            pipe.execute()
        else:
            # 直接删除队列
            self.conn.delete(self._queue_name)

    def dequeue(self, timeout: float = 0) -> Optional['Job']:
        """
        从队列中取出任务
        
        Args:
            timeout: 阻塞超时时间（秒），0 表示非阻塞
            
        Returns:
            Job 对象或 None
        """
        if timeout > 0:
            # 使用阻塞弹出，减少 CPU 占用
            result = self.conn.blpop(self._queue_name, timeout=timeout)
            if not result:
                return None
            job_id = as_string(result[1])  # blpop 返回 (queue_name, value)
        else:
            # 非阻塞弹出
            job_id = self.conn.lpop(self._queue_name)
            if not job_id:
                return None
            job_id = as_string(job_id)
        
        # 更新任务状态为运行中
        self.conn.hset(
            name=f"hive:job:{job_id}",
            mapping={
                "status": Status.RUNNING.value,
                "started_at": get_now()
            }
        )

        job_mapping = self.conn.hgetall(name=f"hive:job:{job_id}")
        return Job._loads(self._transform_job_mapping(job_mapping))
    
    def dequeue_bulk(self, count: int = 10) -> list:
        """
        批量从队列中取出任务
        
        Args:
            count: 取出任务数量
            
        Returns:
            Job 对象列表
        """
        jobs = []
        pipe = self.conn.pipeline()
        
        # 使用管道批量获取
        for _ in range(count):
            pipe.lpop(self._queue_name)
        
        job_ids = pipe.execute()
        
        for job_id in job_ids:
            if not job_id:
                continue
            job_id = as_string(job_id)
            
            # 更新状态
            self.conn.hset(
                name=f"hive:job:{job_id}",
                mapping={
                    "status": Status.RUNNING.value,
                    "started_at": get_now()
                }
            )
            
            job_mapping = self.conn.hgetall(name=f"hive:job:{job_id}")
            job = Job._loads(self._transform_job_mapping(job_mapping))
            jobs.append(job)
        
        return jobs

    def update_status(self, job: 'Job'):
        self.conn.hset(
            name=f"hive:job:{job.job_id}",
            mapping=job.dumps()
        )

    def close(self):
        """关闭 Redis 连接池"""
        if self._redis_client is not None:
            try:
                self._redis_client.close()
            except Exception:
                pass
            self._redis_client = None
        
        if self._pool is not None:
            try:
                self._pool.disconnect()
                self._pool.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出时关闭连接"""
        self.close()
        return False

    def __del__(self):
        """析构时尝试关闭连接"""
        self.close()

    def get_job(self, job_id: str) -> Optional['Job']:
        job_mapping = self.conn.hgetall(name=f"hive:job:{job_id}")
        if not job_mapping:
            return None
        return Job._loads(self._transform_job_mapping(job_mapping))

    @staticmethod
    def _transform_job_mapping(job_mapping: dict):
        job_decode_mapping = {}
        for key, value in job_mapping.items():
            key = as_string(key)
            # 使用安全的反序列化（支持压缩）替代 pickle
            job_decode_mapping[key] = safe_loads_with_compression(value) if key in {'args', 'kwargs', 'result',
                                                                     'error'} else as_string(value)
        return job_decode_mapping

    @property
    def size(self) -> int:
        return self.conn.llen(self._queue_name)

    def ttl(self, job_id: str, ttl: int):
        self.conn.expire(name=f"hive:job:{job_id}", time=ttl)

    def is_empty(self) -> bool:
        """检查队列是否为空"""
        return self.conn.llen(self._queue_name) == 0

    def __repr__(self):
        return f"RedisQueue(name={self._queue_name})"
