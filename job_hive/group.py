from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from job_hive import Job


class Group:
    """
    Represents a group of jobs in the job hive.
    """

    def __init__(self, *args: 'Job') -> None:
        self._jobs = list(args)
        self._locked = False

    def add_task(self, delay_job: 'Job'):
        """
        Adds a job to the group.
        """
        self._jobs.append(delay_job)

    @property
    def jobs(self):
        return self._jobs

    @property
    def lock(self):
        return self._locked

    def __enter__(self):
        if self._locked:
            raise RuntimeError("Group cannot reuse commit")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            raise RuntimeError("Group commit failed") from exc_val
        self._locked = True

    def __len__(self):
        return len(self._jobs)

    def __repr__(self):
        return f"Group(jobs={self._jobs})"
