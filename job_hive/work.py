import os
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import wraps
from typing import TYPE_CHECKING, Optional, Dict, Tuple

from job_hive.core import Status
from job_hive.job import Job
from job_hive.logger import LiveLogger
from job_hive.utils import get_now


def get_optimal_workers(concurrent: Optional[int] = None) -> int:
    """
    根据 CPU 核心数获取最佳的进程池大小
    
    Args:
        concurrent: 用户指定的进程数，若为 None 则自动计算
        
    Returns:
        最佳的进程池工作进程数
    """
    if concurrent is not None and concurrent > 0:
        return concurrent
    
    cpu_count = os.cpu_count()
    if cpu_count is None:
        return 1
    
    # 对于 CPU 密集型任务，使用 CPU 核心数
    # 对于 I/O 密集型任务，可以使用 2-4 倍
    # 这里默认使用 CPU 核心数，留一个核心给主进程和系统
    return max(1, cpu_count - 1)

if TYPE_CHECKING:
    from job_hive.queue import RedisQueue
    from job_hive import Group


class HiveWork:
    def __init__(self, queue: 'RedisQueue'):
        self.logger: Optional[LiveLogger] = None
        self._queue = queue
        self._process_pool: Optional[ProcessPoolExecutor] = None

    def push(self, func, *args, **kwargs) -> str:
        job = Job(func, *args, **kwargs)
        self._queue.enqueue(job)
        return job.job_id

    def pop(self) -> Optional['Job']:
        return self._queue.dequeue()

    def work(self, prefetching: int = 1, waiting: int = 3, concurrent: Optional[int] = None, result_ttl: int = 24 * 60 * 60):
        """
        启动工作进程
        
        Args:
            prefetching: 预取任务数量
            waiting: 无任务时的等待时间（秒）
            concurrent: 并发进程数，若为 None 则根据 CPU 核心数自动计算
            result_ttl: 结果保留时间（秒）
        """
        # 计算最佳进程数
        optimal_workers = get_optimal_workers(concurrent)
        
        # 创建 logger
        self.logger = LiveLogger()
        
        try:
            self.logger.info(r"""
   $$$$\  $$$$$$\  $$$$$$$\          $$\   $$\ $$$$$$\ $$\    $$\ $$$$$$$$\ 
   \__$$ |$$  __$$\ $$  __$$\         $$ |  $$ |\_$$  _|$$ |   $$ |$$  _____|
      $$ |$$ /  $$ |$$ |  $$ |        $$ |  $$ |  $$ |  $$ |   $$ |$$ |      
      $$ |$$ |  $$ |$$$$$$$\ |$$$$$$\ $$$$$$$$ |  $$ |  \$$\  $$  |$$$$$\    
$$\   $$ |$$ |  $$ |$$  __$$\ \______|$$  __$$ |  $$ |   \$$\$$  / $$  __|   
$$ |  $$ |$$ |  $$ |$$ |  $$ |        $$ |  $$ |  $$ |    \$$$  /  $$ |      
\$$$$$$  | $$$$$$  |$$$$$$$  |        $$ |  $$ |$$$$$$\    \$  /   $$$$$$$$\ 
 \______/  \______/ \_______/         \__|  \__|\______|    \_/    \________|

prefetching: {}
waiting: {}
concurrent: {} (CPU cores: {})
result ttl: {}
Started work...
""".format(prefetching, waiting, optimal_workers, os.cpu_count() or "unknown", result_ttl))
            
            self._process_pool = ProcessPoolExecutor(max_workers=optimal_workers)
            
            # 使用 as_completed 优化结果处理
            futures: Dict = {}
            shutdown_requested = False
            
            def handle_completed_job(future, job):
                """处理完成的任务"""
                job.query["ended_at"] = get_now()
                try:
                    job.query["result"] = str(future.result())
                    job.query["status"] = Status.SUCCESS.value
                    self.logger.info(f"Successes job: {job.job_id}")
                    self._queue.ttl(job.job_id, result_ttl)
                except Exception as e:
                    job.query["error"] = "{}\n{}".format(e, traceback.format_exc())
                    job.query["status"] = Status.FAILURE.value
                    self.logger.error(f"Failures job: {job.job_id}")
                finally:
                    self._queue.update_status(job)
            
            while not shutdown_requested:
                # 1. 提交新任务，直到达到 prefetching 限制
                while len(futures) < prefetching:
                    job = self.pop()
                    if job is None:
                        break
                    
                    self.logger.info(f"Started job: {job.job_id}")
                    future = self._process_pool.submit(job)
                    futures[future] = job
                    self._queue.update_status(job)
                
                # 2. 等待完成的任务（使用 as_completed 更高效）
                if futures:
                    # 等待至少一个任务完成
                    done_futures = []
                    try:
                        # 使用 timeout 避免永久阻塞
                        for future in as_completed(futures, timeout=waiting):
                            done_futures.append(future)
                            break  # 处理完一个就继续
                    except TimeoutError:
                        # 超时但没有任务完成，继续检查是否需要提交新任务
                        pass
                    
                    # 处理已完成的任务
                    for future in done_futures:
                        job = futures.pop(future)
                        handle_completed_job(future, job)
                else:
                    # 没有运行中的任务，等待一下
                    time.sleep(waiting)
                    
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested, finishing pending jobs...")
            shutdown_requested = True
        finally:
            # 处理剩余的任务
            if futures:
                self.logger.info(f"Waiting for {len(futures)} pending jobs to complete...")
                for future in as_completed(futures):
                    job = futures[future]
                    handle_completed_job(future, job)
            
            # 关闭进程池
            if self._process_pool is not None:
                self.logger.info("Shutting down process pool...")
                self._process_pool.shutdown(wait=True)
                self._process_pool = None
            
            # 关闭 logger
            if self.logger is not None:
                self.logger.close()
                self.logger = None

    def get_job(self, job_id: str) -> Optional['Job']:
        return self._queue.get_job(job_id)

    def wait(self, job_id: str) -> Optional['Job']:
        while True:
            job: Job = self.get_job(job_id)
            if job is None: return None
            if job.status in (Status.SUCCESS, Status.FAILURE):
                return job
            time.sleep(5)

    def task(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs) -> str:
                return self.push(func, *args, **kwargs)

            return wrapper

        return decorator

    def delay_task(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs) -> 'Job':
                job = Job(func, *args, **kwargs)
                return job

            return wrapper

        return decorator

    def group_commit(self, group: 'Group'):
        """
        Commit a group of jobs to the queue.
        """
        with group:
            self._queue.enqueue(*group.jobs)
        return group

    def __len__(self) -> int:
        return self._queue.size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出时确保资源释放"""
        self._cleanup()
        return False  # 不抑制异常

    def _cleanup(self):
        """清理资源"""
        # 关闭进程池
        if self._process_pool is not None:
            try:
                self._process_pool.shutdown(wait=True)
            except Exception:
                pass  # 忽略关闭时的异常
            finally:
                self._process_pool = None
        
        # 关闭 logger
        if self.logger is not None:
            try:
                self.logger.close()
            except Exception:
                pass
            finally:
                self.logger = None

    def close(self):
        """显式关闭资源"""
        self._cleanup()

    def __del__(self):
        """析构时尝试清理资源（不保证调用）"""
        self._cleanup()

    def __repr__(self):
        return f"<HiveWork queue={self._queue}>"
