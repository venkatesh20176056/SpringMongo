import decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging
from datetime import datetime, timedelta
from typing import Optional, List
from paytm.job.job import Job, Stage, Status, JobType
from paytm.time.time import format_date, format_time_utc, as_utc, parse_time_utc, require_tzinfo


_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def _job_to_dict(job: Job) -> dict:
    return {
        'date_utc': format_date(as_utc(job.timestamp)),
        'id': job.id,
        'timestamp': format_time_utc(job.timestamp),
        'job_type': job.job_type.name,
        'metadata': job.metadata,
        'stages': [_stage_to_dict(stage) for stage in job.stages],
        'status': job.status.name
    }


def _job_from_dict(d: dict) -> Job:

    _log.info("dict - _job_from_dict")
    _log.info(d)
    return Job(
        id=d['id'],
        timestamp=parse_time_utc(d['timestamp']),
        job_type=JobType[d['job_type']],
        metadata=_replace_decimals(d['metadata']),
        stages=[_stage_from_dict(stage) for stage in d['stages']],
        status=Status[d['status']]
    )


def _stage_to_dict(stage: Stage) -> dict:
    return {
        'stage_type': stage.stage_type,
        'request_args': _remove_empty_str_attribs(stage.request_args),
        'status': stage.status.name,
        'attempt_number': stage.attempt_number,
        'execution_id': stage.execution_id,
        'execution_timestamp': _optional_ts_to_str(stage.execution_timestamp),
        'result': _remove_empty_str_attribs(stage.result)
    }


def _stage_from_dict(d: dict) -> Stage:
    return Stage(
        stage_type=d['stage_type'],
        request_args=_replace_decimals(d['request_args']),
        status=Status[d['status']],
        attempt_number=_replace_decimals(d['attempt_number']),
        execution_id=d['execution_id'],
        execution_timestamp=_optional_ts_from_str(d['execution_timestamp']),
        result=_replace_decimals(d['result'])
    )


def _optional_ts_to_str(timestamp: Optional[datetime]) -> Optional[str]:
    if timestamp:
        return format_time_utc(timestamp)
    else:
        return None


def _optional_ts_from_str(timestamp: Optional[str]) -> Optional[datetime]:
    if timestamp:
        return parse_time_utc(timestamp)
    else:
        return None


def _replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = _replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _replace_decimals(v)
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def _remove_empty_str_attribs(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = _remove_empty_str_attribs(obj[i])
        return obj
    elif isinstance(obj, dict):
        remove_keys = []
        for k, v in obj.items():
            if v == '':
                remove_keys.append(k)
            else:
                obj[k] = _remove_empty_str_attribs(v)
        for k in remove_keys:
            del obj[k]
        return obj
    else:
        return obj


class JobDao:

    def __init__(self, aws_region: str, table_name: str, endpoint_url: Optional[str] = None) -> None:
        self.aws_region = aws_region
        self.table_name = table_name

        if endpoint_url:
            self._dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)
        else:
            self._dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        self._table = self._dynamodb.Table(table_name)

    def persist(self, job: Job) -> None:
        _log.info('Persisting job {0}.'.format(job.id))
        self._table.put_item(Item=_job_to_dict(job))

    def load(self, date_utc: str, id: str) -> Optional[Job]:
        response = self._table.get_item(
            Key={
                'date_utc': date_utc,
                'id': id
            }
        )
        if 'Item' in response:
            return _job_from_dict(response['Item'])
        else:
            return None

    def list_unfinished(self, one_day_window_ending_at: datetime) -> List[Job]:
        require_tzinfo(one_day_window_ending_at)
        ending_at = one_day_window_ending_at
        starting_at = ending_at - timedelta(days=1)
        day_of = format_date(as_utc(ending_at))
        previous_day = format_date(as_utc(starting_at))

        jobs = []

        def load_day(day):
            starting_at_str = format_time_utc(starting_at)
            ending_at_str = format_time_utc(ending_at)
            response = self._table.query(
                KeyConditionExpression=Key('date_utc').eq(day),
                FilterExpression=Attr('status').ne(Status.SUCCEEDED.name) &
                    Attr('status').ne(Status.FAILED.name) &
                    Attr('timestamp').gt(starting_at_str) &
                    Attr('timestamp').lte(ending_at_str)
            )
            for job_dict in response['Items']:
                jobs.append(_job_from_dict(job_dict))

        load_day(day_of)
        load_day(previous_day)
        res = sorted(jobs, key=lambda job: job.timestamp)
        job_ids = [job.id for job in res]
        _log.info('Fetched unfinished jobs: {0}.'.format(job_ids))
        return res
