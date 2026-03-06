"""批量任务处理器 - 自动合并细粒度任务"""
import time
import functools
from typing import Callable, List, Any, Optional
from concurrent.futures import ProcessPoolExecutor
import os


class BatchProcessor:
    """
    批量任务处理器
    
    自动将多个细粒度任务合并为一个批次处理，减少进程通信开销。
    适用于任务执行时间 < 100ms 的场景。
    
    Example:
        @work.delay_task()
        @batch_processor.batch(size=100, timeout=1.0)
        def process_item(item_id: int) -> dict:
            return {"id": item_id, "result": item_id * 2}
        
        # 提交1000个任务
        for i in range(1000):
            process_item(i)  # 自动合并为10批次处理
    """
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or max(1, os.cpu_count() - 1)
        self._batches = {}
        self._batch_results = {}
        
    def batch(self, size: int = 100, timeout: float = 1.0) -> Callable:
        """
        批量处理装饰器
        
        Args:
            size: 批次大小，达到此数量立即处理
            timeout: 超时时间（秒），超过此时间即使未满也处理
            
        Returns:
            装饰器函数
        """
        def decorator(func: Callable) -> Callable:
            batch_key = func.__qualname__
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                # 生成唯一任务ID
                task_id = f"{batch_key}_{time.time()}_{id(args)}"
                
                # 创建批次管理器
                if batch_key not in self._batches:
                    self._batches[batch_key] = {
                        'tasks': [],
                        'results': {},
                        'timer': None,
                        'func': func,
                    }
                
                batch = self._batches[batch_key]
                
                # 添加任务到批次
                task_info = {
                    'id': task_id,
                    'args': args,
                    'kwargs': kwargs,
                }
                batch['tasks'].append(task_info)
                
                # 检查是否达到批次大小
                if len(batch['tasks']) >= size:
                    self._process_batch(batch_key)
                
                # 设置超时定时器
                if batch['timer'] is None:
                    batch['timer'] = time.time()
                elif time.time() - batch['timer'] > timeout:
                    self._process_batch(batch_key)
                
                # 等待结果（简化版，实际应该用 Future）
                return self._wait_for_result(batch_key, task_id)
            
            return wrapper
        return decorator
    
    def _process_batch(self, batch_key: str):
        """处理一个批次"""
        batch = self._batches[batch_key]
        tasks = batch['tasks']
        func = batch['func']
        
        if not tasks:
            return
        
        # 清空当前批次
        batch['tasks'] = []
        batch['timer'] = None
        
        # 批量执行任务
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for task in tasks:
                future = executor.submit(func, *task['args'], **task['kwargs'])
                futures.append((task['id'], future))
            
            # 收集结果
            for task_id, future in futures:
                try:
                    result = future.result(timeout=30)
                    batch['results'][task_id] = {'status': 'success', 'data': result}
                except Exception as e:
                    batch['results'][task_id] = {'status': 'error', 'error': str(e)}
    
    def _wait_for_result(self, batch_key: str, task_id: str, timeout: float = 30.0) -> Any:
        """等待任务结果"""
        batch = self._batches[batch_key]
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if task_id in batch['results']:
                result = batch['results'].pop(task_id)
                if result['status'] == 'success':
                    return result['data']
                else:
                    raise Exception(result['error'])
            
            # 检查是否需要处理批次
            if batch['tasks'] and time.time() - batch['timer'] > 1.0:
                self._process_batch(batch_key)
            
            time.sleep(0.01)
        
        raise TimeoutError(f"Task {task_id} timeout")
    
    def flush_all(self):
        """强制处理所有待处理的批次"""
        for batch_key in list(self._batches.keys()):
            self._process_batch(batch_key)


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """将列表分块"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def parallel_map(
    func: Callable[[Any], Any],
    items: List[Any],
    chunk_size: Optional[int] = None,
    max_workers: Optional[int] = None
) -> List[Any]:
    """
    并行处理列表，自动分块和批量处理
    
    Args:
        func: 处理函数
        items: 待处理列表
        chunk_size: 每块大小，默认根据CPU核心数计算
        max_workers: 最大进程数
        
    Returns:
        处理结果列表
        
    Example:
        def process_image(path):
            # 处理图片
            return result
        
        images = ['img1.jpg', 'img2.jpg', ...]  # 1000张图片
        results = parallel_map(process_image, images, chunk_size=50)
    """
    if not items:
        return []
    
    if chunk_size is None:
        # 根据CPU核心数和任务数量计算最佳块大小
        cpu_count = os.cpu_count() or 1
        chunk_size = max(1, len(items) // (cpu_count * 4))
        chunk_size = min(chunk_size, 100)  # 最大100
        chunk_size = max(chunk_size, 10)   # 最小10
    
    chunks = chunk_list(items, chunk_size)
    max_workers = max_workers or max(1, os.cpu_count() - 1)
    
    def process_chunk(chunk):
        """处理一个数据块"""
        return [func(item) for item in chunk]
    
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in futures:
            results.extend(future.result())
    
    return results


class TaskGrouper:
    """
    任务分组器 - 将多个小任务合并为批次任务
    
    适用于任务执行时间 < 50ms 的场景，可以显著减少进程池开销。
    
    Example:
        grouper = TaskGrouper(min_batch_size=50, max_wait_time=0.5)
        
        @grouper.grouped_task
        def process_items(items: List[int]) -> List[dict]:
            return [{"id": i, "result": i * 2} for i in items]
        
        # 提交单个任务（自动合并）
        for i in range(1000):
            process_items.submit(i)
        
        # 获取结果
        results = process_items.get_results()
    """
    
    def __init__(self, min_batch_size: int = 50, max_wait_time: float = 1.0):
        self.min_batch_size = min_batch_size
        self.max_wait_time = max_wait_time
        self._pending_tasks = []
        self._results = {}
        self._task_counter = 0
        self._last_submit_time = time.time()
        
    def grouped_task(self, func: Callable[[List[Any]], List[Any]]) -> Callable:
        """装饰器：将单任务函数转换为批处理函数"""
        
        class GroupedTaskWrapper:
            def __init__(wrapper_self, grouper, func):
                wrapper_self.grouper = grouper
                wrapper_self.func = func
                
            def submit(wrapper_self, item: Any) -> int:
                """提交单个任务，返回任务ID"""
                wrapper_self.grouper._task_counter += 1
                task_id = wrapper_self.grouper._task_counter
                
                wrapper_self.grouper._pending_tasks.append({
                    'id': task_id,
                    'item': item,
                    'submit_time': time.time()
                })
                
                # 检查是否需要立即处理
                if len(wrapper_self.grouper._pending_tasks) >= wrapper_self.grouper.min_batch_size:
                    wrapper_self._flush()
                elif time.time() - wrapper_self.grouper._last_submit_time > wrapper_self.grouper.max_wait_time:
                    wrapper_self._flush()
                
                return task_id
            
            def _flush(wrapper_self):
                """处理待处理的任务"""
                if not wrapper_self.grouper._pending_tasks:
                    return
                
                tasks = wrapper_self.grouper._pending_tasks
                wrapper_self.grouper._pending_tasks = []
                wrapper_self.grouper._last_submit_time = time.time()
                
                items = [t['item'] for t in tasks]
                try:
                    results = wrapper_self.func(items)
                    for task, result in zip(tasks, results):
                        wrapper_self.grouper._results[task['id']] = result
                except Exception as e:
                    for task in tasks:
                        wrapper_self.grouper._results[task['id']] = e
            
            def get_result(wrapper_self, task_id: int, timeout: float = 30.0) -> Any:
                """获取任务结果"""
                start_time = time.time()
                while time.time() - start_time < timeout:
                    # 先尝试刷新
                    if wrapper_self.grouper._pending_tasks:
                        wrapper_self._flush()
                    
                    if task_id in wrapper_self.grouper._results:
                        result = wrapper_self.grouper._results.pop(task_id)
                        if isinstance(result, Exception):
                            raise result
                        return result
                    
                    time.sleep(0.01)
                
                raise TimeoutError(f"Task {task_id} timeout")
            
            def get_results(wrapper_self, task_ids: Optional[List[int]] = None, timeout: float = 30.0) -> List[Any]:
                """获取多个任务结果"""
                if task_ids is None:
                    # 等待所有待处理任务完成
                    while wrapper_self.grouper._pending_tasks:
                        wrapper_self._flush()
                        time.sleep(0.01)
                    task_ids = list(wrapper_self.grouper._results.keys())
                
                return [wrapper_self.get_result(tid, timeout) for tid in task_ids]
            
            def flush(wrapper_self):
                """强制刷新所有待处理任务"""
                wrapper_self._flush()
        
        return GroupedTaskWrapper(self, func)
