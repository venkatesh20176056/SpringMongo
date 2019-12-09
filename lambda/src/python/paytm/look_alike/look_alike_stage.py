import copy
from datetime import datetime
import logging
from typing import List, Optional, Tuple
import os

import boto3
from paytm.azkaban import AzkabanClient
from paytm.job import Stage, Status
from paytm.job.stage_proc.rule_engine_stage import RULE_ENGINE_STAGE_TYPE
from paytm.job.stage_processor import StageProcessor
from paytm.time.time import as_utc
from paytm.look_alike import env_vars

LOOK_ALIKE_STAGE_TYPE = 'look_alike'
LOOK_ALIKE_CMA_TYPE = 'synthetic-lookalike-segment'
s3 = boto3.client('s3')

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def mk_look_alike_stage(segment_id: str, execution_timestamp: datetime, boost_limit: int, tag: str) -> Stage:
    execution_timestamp_str = as_utc(execution_timestamp).strftime('%Y-%m-%d-%H-%M-%SZ')
    return Stage(
        stage_type=LOOK_ALIKE_STAGE_TYPE,
        request_args={
            'segmentId': segment_id,
            'executionTimestamp': execution_timestamp_str,
            'boostLimit': boost_limit,
            'tag': tag
        }
    )


def remove_look_alike_segments_old_cma(scheduled_campaign: dict, campaign_details: dict) -> dict:
    campaign_id: int = scheduled_campaign['id']
    if scheduled_campaign.get('inclusion', {}).get('campaigns'):
        _log.error('Nested campaigns are not supported! Found nested campaigns in campaign {0}.'.format(campaign_id))

    look_alike_segment_ids = []
    look_alike_urls = []
    for segment in campaign_details.get('includedSegments', []):
        if segment.get('description') == LOOK_ALIKE_CMA_TYPE:
            segment_id = segment.get('id')
            if segment_id:
                look_alike_segment_ids.append(segment_id)
            url = segment.get('csvUrl')
            if url:
                look_alike_urls.append(url)

    campaign = copy.deepcopy(scheduled_campaign)
    inclusions = campaign.get('inclusion', {})

    res_uploads = []
    for segment_id in inclusions.get('uploads', []):
        if segment_id not in look_alike_segment_ids:
            res_uploads.append(segment_id)
    if inclusions.get('uploads'):
        inclusions['uploads'] = res_uploads

    res_csvs = []
    for csv in inclusions.get('csvs', []):
        if csv.get('url') not in look_alike_urls:
            res_csvs.append(csv)
    if inclusions.get('csvs'):
        inclusions['csvs'] = res_csvs
    return campaign


def remove_look_alike_segments(scheduled_campaign: dict) -> dict:
    campaign = copy.deepcopy(scheduled_campaign)
    inclusions = campaign.get('inclusion', {})
    keep_segments = []
    for segment in inclusions.get('segments', []):
        if segment.get('description') != LOOK_ALIKE_CMA_TYPE:
            keep_segments.append(segment)

    if inclusions.get('segments'):
        inclusions['segments'] = keep_segments
    return campaign


class LookAlikeStageProc(StageProcessor):

    def __init__(
        self,
        azkaban_client: AzkabanClient,
        project_name: str,
        flow_name: str,
        lookback_days: int,
        segment_size_limit: int,
        output_bucket: str,
        concat_n: int,
        distance_threshold: float,
        kendall_threshold: float,
        tag: str,
        env: str,
        cmaurl: str,
        tenant: str
    ) -> None:
        self.azkaban_client = azkaban_client
        self.project_name = project_name
        self.flow_name = flow_name
        self.lookback_days = lookback_days
        self.segment_size_limit = segment_size_limit
        self.output_bucket = output_bucket
        self.concat_n = concat_n
        self.distance_threshold = distance_threshold
        self.kendall_threshold = kendall_threshold
        self.tag = tag
        self.env = env
        self.cmaurl = cmaurl
        self.tenant = tenant

    @property
    def stage_type(self) -> str:
        return LOOK_ALIKE_STAGE_TYPE

    @property
    def max_concurrent_tasks(self) -> int:
        return 2

    @property
    def max_attempts(self) -> int:
        return 1

    def run(self, finished_stages: List[Stage], stage: Stage) -> Optional[str]:

        _log.info("Stage request_args->{args}".format(args=stage.request_args))

        input_paths = _input_paths(finished_stages)
        if input_paths:
            job_properties = {
                'lookbackDays': self.lookback_days,
                'userBasePath': input_paths,
                'userLimit': stage.request_args.get('boostLimit', self.segment_size_limit),
                'segmentId': stage.request_args['segmentId'],
                'executionTimestamp': stage.request_args['executionTimestamp'],
                'tag': stage.request_args.get('tag', self.tag),
                'concatN': self.concat_n,
                'distanceThreshold': self.distance_threshold,
                'kendallThreshold': self.kendall_threshold,
                'cmaurl':  os.path.join(self.cmaurl, 'clients', self.tenant),
                'bucket': self.output_bucket
            }

            execution_id = self.azkaban_client.submit_job(self.project_name, self.flow_name, job_properties)

            _log.info('execution-spark-submit NOT OFFLINE at look_alike_stage.py = {}.'.format(str(execution_id)))

            return str(execution_id)
        else:
            return None

    def poll(self, stage: Stage) -> Tuple[Status, Optional[dict]]:
        resp = self.azkaban_client.get_status(stage.execution_id)
        new_status = stage.status
        result = None
        if 'status' in resp:
            new_status = _parse_status(resp['status'])
        if new_status == Status.SUCCEEDED:
            args = stage.request_args
            result = self._locate_result_file(args['segmentId'], args['executionTimestamp'])
        return new_status, result

    def _locate_result_file(self, segment_id: str, execution_timestamp: str) -> dict:
        prefix = '{env}/mapfeatures/look-alike/{segment_id}/{timestamp}/{segment_id}_similar_users_scored/'.format(
            env=self.env,
            segment_id=segment_id,
            timestamp=execution_timestamp
        )
        file_key = _find_csv_file(self.output_bucket, prefix)
        if _file_size(self.output_bucket, file_key) == 0:
            return None

        return {
            'file_bucket': self.output_bucket,
            'file_key': file_key
        }


def _parse_status(azkaban_status: str) -> Status:
    try:
        return Status[azkaban_status]
    except KeyError:
        _log.error('Could not parse azkaban status `{0}`.'.format(azkaban_status))
        return Status.FAILED


def _find_csv_file(bucket: str, prefix: str) -> str:
    response = s3.list_objects(Bucket=bucket, Prefix=prefix)
    contents = response.get('Contents', [])
    files = [entry['Key'] for entry in contents]
    csvs = [file for file in files if file.endswith('.csv')]
    if len(csvs) != 1:
        raise Exception('Bucket `{0}`, directory `{1}` did not contain exactly one csv file.')
    return csvs[0]


def _file_size(bucket: str, key: str) -> int:
    obj = s3.head_object(Bucket=bucket, Key=key)
    return obj['ContentLength']


def _input_paths(finished_stages: List[Stage]) -> str:
    urls = []
    for stage in finished_stages:
        if stage.stage_type == RULE_ENGINE_STAGE_TYPE:
            urls.extend(_urls_from(stage.result))
    inputs = [_convert_url(url) for url in urls]
    return ','.join(inputs)


def _urls_from(result: dict) -> List[str]:
    urls = []
    for csv in result['csvs']:
        url = csv.get('url')
        if url:
            urls.append(url)
    return urls


def _convert_url(url: str) -> str:
    """
    Converts an `https://` url to `s3a://`
    """
    path_splits = url.replace("https://", "").split("/")
    path_splits = [path for path in path_splits if path]
    bucket_name = path_splits[0].split('.')[0]
    return "s3a://" + "/".join([bucket_name] + path_splits[1:])
