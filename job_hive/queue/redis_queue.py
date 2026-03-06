import pickle
from typing import Optional

from job_hive.queue.base import BaseQueue
from job_hive.core import Status
from job_hive.utils import as_string, get_now
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
    ):
        if name is None:
            raise ValueError('Queue name cannot be None.')
        self._queue_name = f"hive:queue:{name}"
        self._pool: redis.ConnectionPool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
        )

    @property
    def conn(self):
        return redis.Redis(connection_pool=self._pool)

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
        for job_id in self.conn.lpop(self._queue_name, 0):
            self.conn.hdel(
                name=f"hive:job:{job_id}"
            )

    def dequeue(self) -> Optional['Job']:
        job_id = self.conn.lpop(self._queue_name)
        if not job_id:
            return None
        job_id = as_string(job_id)
        self.conn.hset(
            name=f"hive:job:{job_id}",
            mapping={
                "status": Status.RUNNING.value,
                "started_at": get_now()
            }
        )

        job_mapping = self.conn.hgetall(name=f"hive:job:{job_id}")
        return Job._loads(self._transform_job_mapping(job_mapping))

    def update_status(self, job: 'Job'):
        self.conn.hset(
            name=f"hive:job:{job.job_id}",
            mapping=job.dumps()
        )

    def close(self):
        self._pool.close()

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
            job_decode_mapping[key] = pickle.loads(value) if key in {'args', 'kwargs', 'result',
                                                                     'error'} else as_string(value)
        return job_decode_mapping

    @property
    def size(self) -> int:
        return self.conn.llen(self._queue_name)

    def ttl(self, job_id: str, ttl: int):
        self.conn.expire(name=f"hive:job:{job_id}", time=ttl)

    def is_empty(self) -> bool:
        return bool(self.conn.llen(self._queue_name))

    def __repr__(self):
        return f"RedisQueue(name={self._queue_name})"
