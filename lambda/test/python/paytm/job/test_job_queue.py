from datetime import datetime, timezone
import unittest
from unittest.mock import MagicMock

from paytm.job import Job, JobType, Status
from paytm.job.job_queue import JobQueue
from paytm.job.stub_factory import STUB_STAGE_TYPE, mk_job, mk_stage

FIRST_STAGE = STUB_STAGE_TYPE
SECOND_STAGE = 'second_stage_type'
MAX_ATTEMPTS = 3
MAX_CONCURRENT_TASKS = 4


class TestJobQueue(unittest.TestCase):

    def test_run_iteration(self):
        now = datetime.now(timezone.utc)
        jobs = [mk_job(1, now), mk_job(2, now)]
        jobs[0].job_type = JobType.DAILY
        jobs[1].job_type = JobType.PUSH

        job_queue = JobQueue(MagicMock())
        job_queue.job_dao.list_unfinished.return_value = jobs
        job_queue._update_jobs = MagicMock()
        job_queue._run_jobs = MagicMock()

        job_queue.run_iteration(now)
        job_queue._run_jobs.assert_called_once_with([jobs[1], jobs[0]])

    def test_poll_job(self):
        now = datetime.now(timezone.utc)
        job = mk_job(1, now)
        job.status = Status.RUNNING
        job.stages[0].status = Status.SUCCEEDED
        job.stages[1].status = Status.RUNNING

        job_queue = _mk_multistage_queue()
        job_queue.stage_processors[FIRST_STAGE].poll.return_value = Status.RUNNING, None

        self.assertFalse(job_queue._poll_job(job))
        job_queue.stage_processors[FIRST_STAGE].poll.assert_called_once()

    def test_poll_retriable_task(self):
        now = datetime.now(timezone.utc)
        job = mk_job(1, now)
        job.status = Status.RUNNING
        job.stages[0].status = Status.RUNNING
        job.stages[0].attempt_number = MAX_ATTEMPTS - 1

        def mock_poll(_):
            return Status.FAILED, None

        job_queue = _mk_multistage_queue()
        job_queue.stage_processors[FIRST_STAGE].poll = mock_poll

        self.assertTrue(job_queue._poll_job(job))
        self.assertEqual(job.status, Status.RUNNING)

    def test_poll_failed_task(self):
        now = datetime.now(timezone.utc)
        job = mk_job(1, now)
        job.status = Status.RUNNING
        job.stages[0].status = Status.RUNNING
        job.stages[0].attempt_number = MAX_ATTEMPTS

        def mock_poll(_):
            return Status.FAILED, None

        job_queue = _mk_multistage_queue()
        job_queue.stage_processors[FIRST_STAGE].poll = mock_poll

        self.assertTrue(job_queue._poll_job(job))
        self.assertEqual(job.status, Status.FAILED)

    def test_run_job_first_stage_seq(self):
        now = datetime.now(timezone.utc)
        job = _mk_multistage_job(now)

        def mock_run_stage(_arg1, _arg2, stage):
            stage.status = Status.RUNNING
            return True

        job_queue = _mk_multistage_queue()
        job_queue._run_stage = mock_run_stage

        available_capacity = {FIRST_STAGE: MAX_CONCURRENT_TASKS, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        job_queue._run_job(available_capacity, job)

        job_queue.job_dao.persist.assert_called_with(job)
        self.assertEqual(job_queue.job_dao.persist.call_count, 2)
        self.assertEqual(job.status, Status.RUNNING)

    def test_run_job_second_stage_seq(self):
        now = datetime.now(timezone.utc)
        job = _mk_multistage_job(now)
        job.status = Status.RUNNING
        job.stages[0].status = Status.SUCCEEDED
        job.stages[1].status = Status.SUCCEEDED

        job_queue = _mk_multistage_queue()
        job_queue._run_stage = MagicMock(return_value=True)

        available_capacity = {FIRST_STAGE: MAX_CONCURRENT_TASKS, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        job_queue._run_job(available_capacity, job)

        job_queue.job_dao.persist.assert_called_once_with(job)
        job_queue._run_stage.assert_called_once_with(available_capacity, [job.stages[0], job.stages[1]], job.stages[2])

    def test_run_job_detected_finished(self):
        now = datetime.now(timezone.utc)
        job = _mk_multistage_job(now)
        job.status = Status.RUNNING
        job.stages[0].status = Status.SUCCEEDED
        job.stages[1].status = Status.SUCCEEDED
        job.stages[2].status = Status.SUCCEEDED

        job_queue = _mk_multistage_queue()
        available_capacity = {FIRST_STAGE: MAX_CONCURRENT_TASKS, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        job_queue._run_job(available_capacity, job)

        job_queue.job_dao.persist.assert_called_once_with(job)
        self.assertEqual(job.status, Status.SUCCEEDED)

    def test_run_stage(self):
        finished_stages = [mk_stage(FIRST_STAGE), mk_stage(FIRST_STAGE)]
        finished_stages[0].status = Status.SUCCEEDED
        finished_stages[1].status = Status.SUCCEEDED
        finished_stages[1].result = {'the answer': 42}
        stage = mk_stage(FIRST_STAGE)
        execution_id = 'execution_id_1'

        job_queue = _mk_multistage_queue()
        job_queue.stage_processors[FIRST_STAGE].run.return_value = execution_id
        available_capacity = {FIRST_STAGE: MAX_CONCURRENT_TASKS, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        job_queue._run_stage(available_capacity, finished_stages, stage)

        self.assertEqual(stage.status, Status.RUNNING)
        self.assertEqual(stage.execution_id, execution_id)
        self.assertTrue(stage.execution_timestamp is not None)
        self.assertEqual(stage.attempt_number, 1)
        remaining_capacity = {FIRST_STAGE: MAX_CONCURRENT_TASKS - 1, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        self.assertEqual(available_capacity, remaining_capacity)

    def test_calculate_stage_capacities(self):
        now = datetime.now(timezone.utc)
        jobs = [mk_job(1, now), mk_job(2, now)]
        jobs[0].stages[0].status = Status.RUNNING
        jobs[1].stages[0].status = Status.SUCCEEDED
        jobs[1].stages[1].status = Status.RUNNING

        job_queue = _mk_multistage_queue()
        result = job_queue._calculate_stage_capacities(jobs)
        expected = {FIRST_STAGE: MAX_CONCURRENT_TASKS - 2, SECOND_STAGE: MAX_CONCURRENT_TASKS}
        self.assertEqual(result, expected)


def _mk_multistage_job(timestamp: datetime) -> Job:
    job = mk_job(1, timestamp, stage_count=3)
    job.stages[2].stage_type = SECOND_STAGE
    return job


def _mk_multistage_queue() -> JobQueue:
    stage1_processor = MagicMock()
    stage1_processor.stage_type = FIRST_STAGE
    stage1_processor.max_concurrent_tasks = MAX_CONCURRENT_TASKS
    stage1_processor.max_attempts = MAX_ATTEMPTS

    stage2_processor = MagicMock()
    stage2_processor.stage_type = SECOND_STAGE
    stage2_processor.max_concurrent_tasks = MAX_CONCURRENT_TASKS
    stage2_processor.max_attempts = MAX_ATTEMPTS

    return JobQueue(MagicMock(), stage1_processor, stage2_processor)
