from typing import List, Optional
from datetime import datetime
from enum import Enum


class JobType(Enum):
    PUSH = 1
    DAILY = 2


class Status(Enum):
    PENDING = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4


class Stage:

    def __init__(
        self,
        stage_type: str,
        request_args: dict,
        status: Status = Status.PENDING,
        attempt_number: int = 0,
        execution_id: Optional[str] = None,
        execution_timestamp: Optional[datetime] = None,
        result: Optional[dict] = None
    ) -> None:
        self.stage_type = stage_type
        self.request_args = request_args
        self.status = status
        self.attempt_number = attempt_number
        self.execution_id = execution_id
        self.execution_timestamp = execution_timestamp
        self.result = result

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)

    def unfinished(self) -> bool:
        return self.status != Status.SUCCEEDED and self.status != Status.FAILED


class Job:

    def __init__(
        self,
        id: str,
        timestamp: datetime,
        job_type: JobType,
        metadata: dict,
        stages: List[Stage],
        status: Status = Status.PENDING,
    ) -> None:
        self.id = id
        self.timestamp = timestamp
        self.job_type = job_type
        self.metadata = metadata
        self.stages = stages
        self.status = status

        # job has camp_type field in it
        # if not, job_dao place 'OTHERS' in camp_type field

    def unfinished(self) -> bool:
        return self.status != Status.SUCCEEDED and self.status != Status.FAILED

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)
