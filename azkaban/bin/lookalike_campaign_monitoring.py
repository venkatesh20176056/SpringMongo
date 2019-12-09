import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict

import boto3
import requests
from boto3.dynamodb.conditions import Key, Attr
from requests.exceptions import HTTPError
from botocore.exceptions import ClientError
import time
import argparse
import sys
import pytz

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)

_dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
_table = _dynamodb.Table('map-lookalike-jobs-prod')
_client = boto3.client('dynamodb', region_name='ap-south-1')
_paginator = _client.get_paginator('scan')


def read_requests(url_str: str) -> List[dict]:
    """
    Gets all the CMA campaigns that have lookalike campaigns in started state
    :param url_str: CMA url
    :return: campaigns json
    """
    try:
        params = {
            'hasAudienceBoost': 'true',
            'state': 'started'
        }
        resp = requests.get(url_str, params=params)
        resp.raise_for_status()
    except HTTPError as httpErr:
        print("Error - {error}".format(error=httpErr))
        sys.exit(1)

    else:
        response = resp.json()
        res_json_list = []
        for i in response:
            res_map = {'id': i['id'],
                       'campaign_name': i['name'],
                       'campaign_creationTS': i['createdAt'],
                       'triggerType': i['scheduling']['triggerType'],
                       'firstOccurrence': i['scheduling']['firstOccurrence'],
                       'cron': i['scheduling']['cron']}
            included_segments = i['includedSegments']
            res_map['lookalikeSegment'] = {}
            for j in included_segments:
                if j['description'] == 'synthetic-lookalike-segment':
                    res_map['lookalikeSegment']['segment_updateTS'] = j['updatedAt']
                    res_map['lookalikeSegment']['csvUrl'] = j['csvUrl']
                    res_map['lookalikeSegment']['segmentName'] = j['name']
            res_json_list.append(res_map)

        return res_json_list


def unifinished_jobs() -> Dict[int, dict]:
    """
    Gets all pending jobs in the last day from DynamoDB
    :return: Dict of campaign data
    Here, None is represented as '-1'
    """
    date = str(datetime.now(pytz.utc).date() - timedelta(days=1))
    response = _table.query(
        KeyConditionExpression=Key('date_utc').eq(date),
        FilterExpression=Attr('status').ne('SUCCEEDED')
    )
    id_map = {}
    for resp in response['Items']:
        key = int(resp['stages'][0]['request_args']['campaign_id'])
        in_progress_map = {'id': key, 'stages': []}
        for stage in resp['stages']:
            stage_map = {
                'executionTS': str(stage['execution_timestamp']),
                'attempt_number': int(stage['attempt_number']),
                'stage_type': stage['stage_type'],
                'status': stage['status']
            }
            in_progress_map['stages'].append(stage_map)
        in_progress_map['last_succeeded_in_3days'] = '-1'
        in_progress_map['execution_id'] = '-1'
        id_map[key] = in_progress_map

    return id_map


def get_last_succeeded(campaign_ids: List[int]) -> Dict[int, dict]:
    """
    Given a list of campaign ids, retrieves all those campaigns that had previously succeeded in the last 3 days.
    Most recent successful campaign data is returned
    :param campaign_ids: List of campaign ids
    :return: Dict of recently succeeded campaign data
    """

    mid_value = len(campaign_ids) // 2

    id_list = [campaign_ids[:mid_value]]+[campaign_ids[mid_value:]]

    id_map = {}

    for ids in id_list:

        date = str(datetime.now(pytz.utc).date() - timedelta(days=3))

        inter_query_str = ' OR '.join('contains(#id_attr, :id_val{i})'.format(i=i) for i in range(len(ids)))

        query_str = '({query}) AND #status_attr = :status_val AND #date_attr >= :date_val'.format(query=inter_query_str)

        attribute_dict = {}

        for i in range(len(ids)):
            key_val = ':id_val{i}'.format(i=i)
            attribute_dict[key_val] = {'S': str(ids[i])}

        attribute_dict[':status_val'] = {'S': 'SUCCEEDED'}
        attribute_dict[':date_val'] = {'S': date}

        operation_params = {
            'TableName': 'map-lookalike-jobs-prod',
            'FilterExpression': query_str,
            'ExpressionAttributeValues': attribute_dict,
            'ExpressionAttributeNames': {
                '#id_attr': 'id',
                '#status_attr': 'status',
                '#date_attr': 'date_utc'
            }
        }

        max_retries = 5
        result_list = []
        while max_retries > 0:
            try:
                for page in _paginator.paginate(**operation_params):
                    result_list.append((page['Items']))
                break
            except ClientError as err:
                if err.response['Error']['Code'] != 'ProvisionedThroughputExceededException':
                    raise
                time.sleep(30)
                max_retries -= 1

        for i in result_list:
            for k in i:
                key = int(k['stages']['L'][0]['M']['request_args']['M']['campaign_id']['N'])
                key_map = {'id': key, 'stages': []}
                for stage in k['stages']['L']:
                    stage_map = {
                        'executionTS': stage['M']['execution_timestamp']['S'],
                        'attempt_number': stage['M']['attempt_number']['N'],
                        'stage_type': stage['M']['stage_type']['S'],
                        'status': stage['M']['status']['S']
                    }
                    key_map['stages'].append(stage_map)

                dt_val = datetime.strptime(k['date_utc']['S'], '%Y-%m-%d')
                exec_id = k['stages']['L'][1]['M']['execution_id']['S']

                if key in id_map:
                    val = id_map[key]
                    key_cmp = datetime.strptime(val['last_succeeded_in_3days'], '%Y-%m-%d')
                    if key_cmp < dt_val:
                        key_map['last_succeeded_in_3days'] = dt_val.strftime('%Y-%m-%d')
                        key_map['execution_id'] = exec_id
                else:
                    key_map['last_succeeded_in_3days'] = dt_val.strftime('%Y-%m-%d')
                    key_map['execution_id'] = exec_id
                    id_map[key] = key_map

    return id_map


def __combine_result(cma_result, pending_map, last_succeeded_map) -> List[dict]:
    """
    Combines all the intermediate json for persist
    :param cma_result: CMA current campaign json
    :param pending_map: pending jobs json
    :param last_succeeded_map: last succeeded jobs json
    :return: campaign data json
    """
    for k, v in pending_map.items():
        if k not in last_succeeded_map:
            last_succeeded_map[k] = v

    stage_result = [i for i in last_succeeded_map.values()]

    for p_res in stage_result:
        for l_res in cma_result:
            if p_res['id'] == l_res['id']:
                l_res.update(p_res)

    return cma_result


def run(url: str) -> List[dict]:
    """
    Runner method
    :param url: CMA url
    :return: campaign data
    """
    cma_json = read_requests(url)
    pending_jobs = unifinished_jobs()
    lookalike_ids = [i['id'] for i in cma_json]
    lookalike_id_results = get_last_succeeded(lookalike_ids)
    intermediate_results = {
        'cma_result': cma_json,
        'pending_map': pending_jobs,
        'last_succeeded_map': lookalike_id_results
    }
    return __combine_result(**intermediate_results)


if __name__ == '__main__':
    _log.info('Begin Execution')

    parser = argparse.ArgumentParser(description='Dynamo Monitoring parser')
    parser.add_argument('--cma_url', type=str, help='CMA url for lookalike audience')
    parser.add_argument('--bucket', type=str, help='S3 Bucket')
    parser.add_argument('--path', type=str, help='S3 file path')
    args = parser.parse_args()

    file_date = datetime.now(pytz.utc).strftime('%Y-%m-%d')
    s3_file_path = '{path}/{date}/lookalike_jobs_info.json'.format(path=args.path, date=file_date)

    result = run(args.cma_url)
    print(json.dumps(result, indent=2))

    s3 = boto3.resource('s3', region_name='ap-south-1')
    s3Object = s3.Object(args.bucket, s3_file_path)
    s3Object.put(
        Body=(bytes(json.dumps(result).encode('UTF-8')))
    )

    _log.info('End of Execution')
