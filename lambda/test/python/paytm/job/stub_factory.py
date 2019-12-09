from datetime import datetime

from paytm.job import Job, JobType, Stage, Status

STUB_STAGE_TYPE = 'test_stage'


def mk_stage(stage_type: str = STUB_STAGE_TYPE) -> Stage:
    return Stage(
        stage_type=stage_type,
        request_args={},
        status=Status.PENDING,
        attempt_number=0,
        execution_id=None,
        execution_timestamp=None,
        result=None
    )


def mk_job(id: int, timestamp: datetime, stage_count: int = 2) -> Job:
    i = str(id)
    return Job(
        id='id_' + i,
        timestamp=timestamp.replace(microsecond=0),
        job_type=JobType.PUSH,
        metadata={},
        stages=[mk_stage() for _ in range(0, stage_count)],
        status=Status.PENDING
    )
