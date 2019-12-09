import logging
from typing import List, Optional, Tuple
from uuid import uuid4

from paytm.async_result.async_result_dao import AsyncResultDao
from paytm.azkaban import AzkabanClient
from paytm.cma import CmaClient
from paytm.job import Stage, Status
from paytm.job.stage_processor import StageProcessor
from paytm.look_alike.look_alike_stage import LOOK_ALIKE_STAGE_TYPE, LOOK_ALIKE_CMA_TYPE
from paytm.look_alike import env_vars

UPDATE_SEGMENTS_STAGE_TYPE = 'update_segments'

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def mk_update_segments_stage(campaign_id: int) -> Stage:
    return Stage(
        stage_type=UPDATE_SEGMENTS_STAGE_TYPE,
        request_args={
            'campaign_id': campaign_id,
            'upload_url_id': str(uuid4())
        }
    )


class UpdateSegmentsStageProc(StageProcessor):

    def __init__(
        self,
        azkaban_client: AzkabanClient,
        project_name: str,
        flow_name: str,
        cma_client: CmaClient,
        async_result_dao: AsyncResultDao
    ) -> None:
        self.azkaban_client = azkaban_client
        self.project_name = project_name
        self.flow_name = flow_name
        self.cma_client = cma_client
        self.async_result_dao = async_result_dao

    @property
    def stage_type(self) -> str:
        return UPDATE_SEGMENTS_STAGE_TYPE

    @property
    def max_concurrent_tasks(self) -> int:
        return 4

    @property
    def max_attempts(self) -> int:
        return 3

    def run(self, finished_stages: List[Stage], stage: Stage) -> Optional[str]:
        segment_file = _get_segment_file(finished_stages)
        if segment_file is None:
            _update_segments(self.cma_client, stage.request_args['campaign_id'], None)
            return None
        else:
            job_properties = {
                'cmaUrl': self.cma_client.url,
                'fileBucket': segment_file['file_bucket'],
                'fileKey': segment_file['file_key'],
                'awsRegion': self.async_result_dao.aws_region,
                'tableName': self.async_result_dao.table_name,
                'resultId': stage.request_args['upload_url_id']
            }

            execution_id = self.azkaban_client.submit_job(self.project_name, self.flow_name, job_properties)
            return str(execution_id)

    def poll(self, stage: Stage) -> Tuple[Status, Optional[dict]]:
        resp = self.azkaban_client.get_status(stage.execution_id)
        new_status = stage.status
        if 'status' in resp:
            new_status = _parse_status(resp['status'])
        if new_status == Status.SUCCEEDED:
            args = stage.request_args
            self._update_segments_with_async_result(args['campaign_id'], args['upload_url_id'])
        return new_status, None

    def _update_segments_with_async_result(self, campaign_id: int, upload_url_id: str) -> None:
        upload_url = self.async_result_dao.load(upload_url_id)
        if upload_url is None:
            raise ValueError(
                'Could not find CMA upload_url for campaign {1}, result_id {0}.'.format(campaign_id, upload_url_id))
        _update_segments(self.cma_client, campaign_id, upload_url)
        self.async_result_dao.remove(upload_url_id)


def _parse_status(azkaban_status: str) -> Status:
    try:
        return Status[azkaban_status]
    except KeyError:
        _log.error('Could not parse azkaban status `{0}`.'.format(azkaban_status))
        return Status.FAILED


def _get_segment_file(finished_stages: List[Stage]) -> Optional[dict]:
    look_alike_stages = [s for s in finished_stages if s.stage_type == LOOK_ALIKE_STAGE_TYPE]
    assert len(look_alike_stages) == 1
    return look_alike_stages[0].result


def _update_segments(cma_client: CmaClient, campaign_id: int, upload_url: Optional[str]) -> None:
    def entry_point():
        segment_id = None
        _log.info('Updating segments for campaign {0} with uploaded_url {1}.'.format(campaign_id, upload_url))
        if upload_url is not None:
            segment_id = create_segment(upload_url)
        segments = cma_client.get_campaign_segments(campaign_id)
        old_segment_ids, included_segment_ids, excluded_segment_ids = update_segments(segments, segment_id)
        cma_client.update_campaign_segments(campaign_id, included_segment_ids, excluded_segment_ids)
        delete_old_segments(old_segment_ids)

    def create_segment(upload_url):
        segment_name = 'lookalike_for_{0}_{1}'.format(campaign_id, uuid4())
        return cma_client.create_synthetic_segment(segment_name, LOOK_ALIKE_CMA_TYPE, upload_url)

    def update_segments(segments, segment_id):
        included_segments = segments['included']
        excluded_segments = segments['excluded']

        old_segments = [s for s in included_segments if s['description'] == LOOK_ALIKE_CMA_TYPE]
        old_segments_ids = [s['id'] for s in old_segments]
        included_segments_ids = [s['id'] for s in included_segments]
        included_segments_ids = [id for id in included_segments_ids if id not in old_segments_ids]
        if segment_id is not None:
            included_segments_ids.append(segment_id)
        excluded_segments_ids = [s['id'] for s in excluded_segments]
        return old_segments_ids, included_segments_ids, excluded_segments_ids

    def delete_old_segments(old_segment_ids):
        for old_segment_id in old_segment_ids:
            cma_client.delete_segment(old_segment_id)

    entry_point()
