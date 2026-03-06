from logging import Logger, Formatter, StreamHandler, DEBUG, INFO, WARNING, ERROR, CRITICAL
from typing import Union, Optional
import sys


class LiveLogger(Logger):
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

        if file:
            from logging.handlers import RotatingFileHandler
            fh = RotatingFileHandler(file, maxBytes=10485760, backupCount=5)
            fh.setFormatter(fmt)
            self.addHandler(fh)

        sh = StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(fmt)
        self.addHandler(sh)
