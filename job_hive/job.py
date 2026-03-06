import inspect
import json
import pickle
import uuid
from typing import Optional, Any

from job_hive.core import Status
from job_hive.utils import import_attribute, get_now


class Job:
    def __init__(self, func, *args, **kwargs):
        self.job_id = str(uuid.uuid4())
        self.func = self._get_func_path(func)
        self._args = args
        self.query = {
            "status": Status.PENDING.value,
            "created_at": get_now(),
        }
        self._kwargs = kwargs

    @staticmethod
    def _get_func_path(func):
        if inspect.isfunction(func) or inspect.isbuiltin(func):
            return '{0}.{1}'.format(func.__module__, func.__qualname__)
        elif isinstance(func, str):
            return func
        else:
            raise TypeError('Expected a callable or a string, but got: {0}'.format(func))

    @property
    def created_at(self) -> Optional[str]:
        return self.query.get("created_at", '')

    @property
    def ended_at(self) -> Optional[str]:
        return self.query.get("ended_at", '')

    @property
    def started_at(self) -> Optional[str]:
        return self.query.get("started_at", '')

    @property
    def status(self) -> Optional[Status]:
        return Status(self.query.get("status", Status.PENDING.value))

    @property
    def result(self) -> Any:
        return self.query.get("result", None)

    @property
    def error(self) -> Any:
        return self.query.get("error", None)

    def dumps(self) -> dict:
        return {
            "job_id": self.job_id,
            "func": self.func,
            "args": self._dumps(self._args),
            "kwargs": self._dumps(self._kwargs),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "result": self._dumps(self.result),
            "error": self._dumps(self.error),
        }

    @staticmethod
    def _loads(obj: dict) -> 'Job':
        job = Job(
            obj["func"],
            *obj["args"],
            **obj["kwargs"],
        )
        job.job_id = obj["job_id"]

        for key in ["created_at", "ended_at", "started_at", "status", "result", "error"]:
            job.query[key] = obj.get(key, '')
        return job

    @staticmethod
    def _dumps(obj: Any) -> bytes:
        return pickle.dumps(obj)

    @property
    def detail(self):
        return json.dumps({
            "job_id": self.job_id,
            "func": self.func.__name__,
            "args": str(self._args),
            "kwargs": str(self._kwargs),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
        }, indent=4, ensure_ascii=False)

    def __call__(self, *args, **kwargs):
        func = import_attribute(self.func)
        if hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        return func(*self._args, **self._kwargs)

    def __repr__(self):
        return f"[Job {self.job_id}]"