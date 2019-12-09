from datetime import datetime, timezone
import logging
from typing import Dict, List

from paytm.job import Job, Stage, Status
from paytm.job.job_dao import JobDao
from paytm.job.stage_processor import StageProcessor
from paytm.look_alike import env_vars

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


class JobQueue:

    def __init__(self, job_dao: JobDao, *stage_processors: StageProcessor) -> None:
        self.job_dao = job_dao
        self.stage_processors = {p.stage_type: p for p in stage_processors}

    def run_iteration(self, now: datetime):
        jobs = self.job_dao.list_unfinished(now)
        self._update_jobs(jobs)
        prioritized_jobs = sorted(jobs, key=lambda job: job.job_type.value)
        self._run_jobs(prioritized_jobs)

    def _update_jobs(self, jobs: List[Job]) -> None:
        for job in jobs:
            if job.status == Status.RUNNING:
                job_updated = self._poll_job(job)
                if job_updated:
                    self.job_dao.persist(job)

    def _run_jobs(self, jobs: List[Job]) -> None:
        stage_capacities = self._calculate_stage_capacities(jobs)
        for job in jobs:
            if job.unfinished():
                self._run_job(stage_capacities, job)

    def _run_job(self, stage_capacities: Dict[str, int], job: Job) -> None:
        assert job.unfinished()
        unfinished_stage_sequence_type = None
        finished_stages: List[Stage] = []
        for stage in job.stages:
            if stage.unfinished():
                if unfinished_stage_sequence_type:
                    if unfinished_stage_sequence_type != stage.stage_type:
                        break
                else:
                    unfinished_stage_sequence_type = stage.stage_type

                if stage.status == Status.PENDING:
                    stage_started = self._run_stage(stage_capacities, finished_stages, stage)
                    if stage_started:
                        if job.status == Status.PENDING:
                            job.status = Status.RUNNING
                        _log.info(
                            '{0} stage transitioned from {1} to {2}: {3}.'.format(
                                job.id, Status.PENDING, stage.status, stage))
                        self.job_dao.persist(job)

                if stage.status != Status.RUNNING:
                    break
            else:
                finished_stages.append(stage)

        if len(finished_stages) == len(job.stages):
            succeeded_stages = [s for s in finished_stages if s.status == Status.SUCCEEDED]
            if len(succeeded_stages) == len(job.stages):
                job.status = Status.SUCCEEDED
            else:
                job.status = Status.FAILED
            _log.info('{0} completed with {1}.'.format(job.id, job.status))
            self.job_dao.persist(job)

    def _poll_job(self, job: Job) -> bool:
        assert job.status == Status.RUNNING
        updated = False
        for stage in job.stages:
            if stage.status == Status.RUNNING:
                stage_processor = self.stage_processors[stage.stage_type]
                status, result = stage_processor.poll(stage)
                old_status = stage.status
                if old_status != status:
                    stage.status = status
                    stage.result = result
                    _log.info(
                        '{0} stage transitioned from {1} to {2}: {3}.'.format(job.id, old_status, stage.status, stage))
                    updated = True

                if stage.status == Status.FAILED:
                    if stage.attempt_number < stage_processor.max_attempts:
                        stage.status = Status.PENDING
                    else:
                        job.status = Status.FAILED
        return updated

    def _run_stage(self, stage_capacities: Dict[str, int], finished_stages: List[Stage], stage: Stage) -> bool:
        assert stage.status == Status.PENDING
        available_stage_capacity = stage_capacities[stage.stage_type]
        if available_stage_capacity > 0:
            stage_processor = self.stage_processors[stage.stage_type]
            execution_id = stage_processor.run(finished_stages, stage)
            if execution_id:
                stage.status = Status.RUNNING
                stage.execution_id = execution_id
                stage.execution_timestamp = datetime.now(timezone.utc)
                stage.attempt_number += 1
                stage_capacities[stage.stage_type] = available_stage_capacity - 1
            else:
                stage.status = Status.SUCCEEDED
                stage.execution_id = 'skipped_no_input'
                stage.execution_timestamp = datetime.now(timezone.utc)
                stage.attempt_number = 0
            return True
        else:
            return False

    def _calculate_stage_capacities(self, jobs: List[Job]) -> Dict[str, int]:
        res = {}
        for stage_type, stage_processor in self.stage_processors.items():
            tasks_in_progress = self._count_tasks_in_progress(stage_type, jobs)
            res[stage_type] = stage_processor.max_concurrent_tasks - tasks_in_progress
        return res

    def _count_tasks_in_progress(self, stage_type: str, jobs: List[Job]) -> int:
        res = 0
        for job in jobs:
            for stage in job.stages:
                if stage.stage_type == stage_type and stage.status == Status.RUNNING:
                    res += 1
        return res
