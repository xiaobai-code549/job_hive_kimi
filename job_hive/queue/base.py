from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from job_hive.job import Job


class BaseQueue(metaclass=ABCMeta):

    @abstractmethod
    def enqueue(self, *args: 'Job'):
        """
        将作业排队到队列中

        :param args: 作业对象或作业对象列表
        :return:
        """
        pass

    @abstractmethod
    def remove(self, job: 'Job'):
        """
        从队列中移除作业

        :param job: 作业对象
        :return:
        """
        pass

    @abstractmethod
    def dequeue(self) -> Optional['Job']:
        """
        从队列中取出作业

        :return:
        """
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        """
        获取队列中作业数量

        :return:
        """
        pass

    @abstractmethod
    def clear(self):
        """
        清空队列

        :return:
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        判断队列是否为空

        :return:
        """
        pass

    @abstractmethod
    def update_status(self, job: 'Job'):
        """
        更新队列中作业的状态

        :return:
        """
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> Optional['Job']:
        """
        获取队列中作业

        :param job_id:
        :return:
        """
        pass

    @abstractmethod
    def ttl(self, job_id: str, ttl: int):
        """
        设置队列中作业的过期时间

        :param job_id:
        :param ttl:
        :return:
        """
        pass

    @abstractmethod
    def close(self):
        """
        关闭队列

        :return:
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
