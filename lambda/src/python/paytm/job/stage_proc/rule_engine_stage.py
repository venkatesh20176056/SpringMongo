import logging
from typing import List, Optional, Tuple

from paytm.job import Stage, Status
from paytm.job.stage_processor import StageProcessor
from paytm.rule_engine import RuleEngineClient
from requests import HTTPError

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


RULE_ENGINE_STAGE_TYPE = 'rule_engine'


def mk_rule_engine_stage(campaign_id: int, inclusion: dict, exclusion: dict = None) -> Stage:
    if exclusion is None:
        exclusion = dict()

    return Stage(
        stage_type=RULE_ENGINE_STAGE_TYPE,
        request_args={
            'campaign_id': campaign_id,
            'inclusion': inclusion,
            'exclusion': exclusion
        }
    )


class RuleEngineStageProc(StageProcessor):

    def __init__(self, rule_engine_client: RuleEngineClient) -> None:
        self.rule_engine_client = rule_engine_client

    @property
    def stage_type(self) -> str:
        return RULE_ENGINE_STAGE_TYPE

    @property
    def max_concurrent_tasks(self) -> int:
        return 4

    @property
    def max_attempts(self) -> int:
        return 3

    def run(self, finished_stages: List[Stage], stage: Stage) -> Optional[str]:
        args = stage.request_args
        request_id = self.rule_engine_client.create_dataset(
            args['campaign_id'], args['inclusion'], args['exclusion'], ['customer_id']
        )
        return request_id

    def poll(self, stage: Stage) -> Tuple[Status, Optional[dict]]:
        resp = None
        try:
            resp = self.rule_engine_client.get_dataset_status(stage.execution_id)
        except HTTPError as e:
            _log.error('Could not fetch dataset status.', e)

        new_status = stage.status
        result = None
        if 'state' in resp:
            new_status = _parse_status(resp['state'])
        if new_status == Status.SUCCEEDED:
            result = _parse_result(resp)
        return new_status, result


def _parse_status(rule_engine_state: str) -> Status:
    if rule_engine_state == 'SUCCESS':
        return Status.SUCCEEDED
    elif rule_engine_state == 'FAILED':
        return Status.FAILED
    elif rule_engine_state == 'PROCESSING':
        return Status.RUNNING
    else:
        return Status.FAILED


def _parse_result(resp: dict) -> dict:
    job_id = resp.get('job_id')
    csvs = resp.get('csvs', [])
    return {
        'job_id': job_id,
        'csvs': csvs
    }
