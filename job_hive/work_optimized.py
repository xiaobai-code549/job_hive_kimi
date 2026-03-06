"""优化版 HiveWork - 减少 GIL 影响"""
import os
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import wraps
from typing import TYPE_CHECKING, Optional
from threading import Thread
import queue

from job_hive.core import Status
from job_hive.job import Job
from job_hive.logger import LiveLogger
from job_hive.utils import get_now

if TYPE_CHECKING:
    from job_hive.queue import RedisQueue
    from job_hive import Group


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


class HiveWorkOptimized:
    """
    优化版 HiveWork，使用异步结果收集减少 GIL 影响
    """
    def __init__(self, queue: 'RedisQueue'):
        self.logger: Optional[LiveLogger] = None
        self._queue = queue
        self._process_pool: Optional[ProcessPoolExecutor] = None
        self._result_queue = queue.Queue()
        self._shutdown = False

    def push(self, func, *args, **kwargs) -> str:
        job = Job(func, *args, **kwargs)
        self._queue.enqueue(job)
        return job.job_id

    def pop(self) -> Optional['Job']:
        return self._queue.dequeue()

    def work(self, prefetching: int = 1, waiting: int = 3, concurrent: Optional[int] = None, result_ttl: int = 24 * 60 * 60):
        """
        优化版工作模式
        
        Args:
            prefetching: 预取任务数量
            waiting: 无任务时的等待时间（秒）
            concurrent: 并发进程数，若为 None 则根据 CPU 核心数自动计算
            result_ttl: 结果保留时间（秒）
            
        改进点：
        1. 使用 as_completed 替代手动轮询，更高效
        2. 结果处理与任务提交解耦
        3. 减少主线程 CPU 占用
        4. 根据 CPU 核心数自动计算最佳进程数
        """
        # 计算最佳进程数
        optimal_workers = get_optimal_workers(concurrent)
        
        self.logger = LiveLogger()
        self.logger.info(r"""
   $$$$$$\  $$$$$$\  $$$$$$$\          $$\   $$\ $$$$$$\ $$\    $$\ $$$$$$$$\ 
   \__$$ |$$  __$$\ $$  __$$\         $$ |  $$ |\_$$  _|$$ |   $$ |$$  _____|
      $$ |$$ /  $$ |$$ |  $$ |        $$ |  $$ |  $$ |  $$ |   $$ |$$ |      
      $$ |$$ |  $$ |$$$$$$$\ |$$$$$$\ $$$$$$$$ |  $$ |  \$$\  $$  |$$$$$\    
$$\   $$ |$$ |  $$ |$$  __$$\ \______|$$  __$$ |  $$ |   \$$\$$  / $$  __|   
$$ |  $$ |$$ |  $$ |$$ |  $$ |        $$ |  $$ |  $$ |    \$$$  /  $$ |      
\$$$$$$  | $$$$$$  |$$$$$$$  |        $$ |  $$ |$$$$$$\    \$  /   $$$$$$$$\ 
 \______/  \______/ \_______/         \__|  \__|\______|    \_/    \________|

[Optimized Version]
prefetching: {}
waiting: {}
concurrent: {} (CPU cores: {})
result ttl: {}
Started work...
""".format(prefetching, waiting, optimal_workers, os.cpu_count() or "unknown", result_ttl))
        
        self._process_pool = ProcessPoolExecutor(max_workers=optimal_workers)
        
        # 使用 as_completed 更高效地处理结果
        futures = {}
        
        try:
            while not self._shutdown:
                # 1. 提交新任务（如果有空间）
                while len(futures) < prefetching:
                    job = self.pop()
                    if job is None:
                        break
                    
                    self.logger.info(f"Started job: {job.job_id}")
                    future = self._process_pool.submit(job)
                    futures[future] = job
                    self._queue.update_status(job)
                
                # 2. 等待完成的任务（阻塞，释放 GIL）
                if futures:
                    # as_completed 会在有任务完成时立即返回
                    for future in as_completed(futures, timeout=waiting):
                        job = futures.pop(future)
                        self._handle_completed_job(future, job, result_ttl)
                        break  # 处理完一个就继续检查是否需要提交新任务
                else:
                    # 没有运行中的任务，等待一下
                    time.sleep(waiting)
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        finally:
            self._shutdown = True
            self._process_pool.shutdown(wait=True)

    def _handle_completed_job(self, future, job, result_ttl):
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

    def get_job(self, job_id: str) -> Optional['Job']:
        return self._queue.get_job(job_id)

    def wait(self, job_id: str) -> Optional['Job']:
        while True:
            job: Job = self.get_job(job_id)
            if job is None:
                return None
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
        """Commit a group of jobs to the queue."""
        with group:
            self._queue.enqueue(*group.jobs)
        return group

    def __len__(self) -> int:
        return self._queue.size

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._process_pool is None:
            return
        self._process_pool.shutdown()

    def __enter__(self):
        return self

    def __del__(self):
        if self._process_pool is None:
            return
        self._process_pool.shutdown()

    def __repr__(self):
        return f"<HiveWorkOptimized queue={self._queue}>"
