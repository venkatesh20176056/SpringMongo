from datetime import datetime, timezone
import logging
import sys
from typing import Any

import boto3
import requests
from requests import HTTPError

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def upload_file_to_cma(cma_url: str, file_bucket: str, file_key: str, aws_region: str, table_name: str, result_id: str) -> None:
    s3 = boto3.client('s3')
    file_obj = s3.get_object(Bucket=file_bucket, Key=file_key)['Body']
    _log.info('Uploading file to CMA...')
    upload_url = _upload_segment_data(cma_url, file_obj)
    _log.info('Persisting upload_url to DynamoDB...')
    _persist_res_to_dynamo(aws_region, table_name, result_id, upload_url)
    _log.info('Finished successfully.')


def _upload_segment_data(cma_url: str, file_obj: Any) -> str:
    request_url = cma_url + '/files/upload'
    multipart_data = {
        'file': file_obj,
        'uploadType': 'segment'
    }
    response = requests.post(request_url, files=multipart_data)
    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

    return response.json()['url']


def _persist_res_to_dynamo(aws_region: str, res_table: str, result_id: str, res: Any) -> None:
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    table = dynamodb.Table(res_table)
    item = {
        'id': result_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'result': res
    }
    table.put_item(Item=item)


def _parse_sys_argv(_, cma_url, file_bucket, file_key, aws_region, table_name, result_id):
    return [cma_url, file_bucket, file_key, aws_region, table_name, result_id]


if __name__ == '__main__':
    args = _parse_sys_argv(*sys.argv)
    upload_file_to_cma(*args)
