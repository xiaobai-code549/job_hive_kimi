from logging import Logger, Formatter, StreamHandler, DEBUG, INFO, WARNING, ERROR, CRITICAL
from typing import Union, Optional, List
import sys


class LiveLogger(Logger):
    """
    支持上下文管理的日志记录器
    
    使用示例:
        with LiveLogger() as logger:
            logger.info("message")
        # 退出上下文时自动关闭文件句柄
    """
    
    def __init__(self,
                 name: str = 'job_hive',
                 file: Optional[str] = None,
                 logger_level: Union[DEBUG, INFO, WARNING, ERROR, CRITICAL] = INFO,
                 level: int = 0,
                 logger_format: str = '[%(asctime)s] %(levelname)s: %(message)s',
                 ):
        super().__init__(name)
        self.setLevel(logger_level)
        fmt = Formatter(logger_format)
        
        self._file_path = file
        self._handlers_to_close: List = []

        if file:
            from logging.handlers import RotatingFileHandler
            fh = RotatingFileHandler(file, maxBytes=10485760, backupCount=5)
            fh.setFormatter(fmt)
            self.addHandler(fh)
            self._handlers_to_close.append(fh)

        sh = StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(fmt)
        self.addHandler(sh)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时关闭所有文件句柄"""
        self.close()
        return False

    def close(self):
        """显式关闭所有文件句柄"""
        for handler in self._handlers_to_close:
            try:
                handler.close()
            except Exception:
                pass
        self._handlers_to_close.clear()
        
        # 清除所有处理器
        for handler in self.handlers[:]:
            try:
                self.removeHandler(handler)
                handler.close()
            except Exception:
                pass

    def __del__(self):
        """析构时尝试关闭资源"""
        self.close()
