import logging
import sys
from typing import Any, List
import argparse
import os
import uuid
import time

import boto3
import requests
from requests import HTTPError
from upload_csv_from_s3_to_cma import read_body_from_s3
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete
from update_dynamo import connect_dynamo, find_campaign_id, update_status_to_dynamo

'''
flow logic:
1. with upload_url, get new segment ID of lookalike segment
2. add this new segment into compaign inclusionSegment
3. confirm whether the update is reflected via campaing info query
'''


def create_synthetic_segment(cma_url: str, segment_name: str, segment_type: str, uploaded_url: str) -> int:
    request_url = os.path.join(cma_url, 'segments')
    request_json = {
        "csvUrl": uploaded_url,
        "description": segment_type,
        "name": segment_name,
        "type": "CSV_BASED"
    }
    response = requests.post(request_url, json=request_json)

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

    res = response.json()
    print('Created synthetic segment: {0}.'.format(res))
    return res['id']


def get_campaign_segments(cma_url: str, campaign_id: int) -> dict:
    request_url = os.path.join(cma_url, 'campaigns/{0}'.format(campaign_id))
    print("request URL= {0}".format(request_url))
    response = requests.get(request_url, headers={'Cache-Control': 'no-cache'})

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

    res = response.json()

    print('Loaded campaign {0} segments: {1}.'.format(campaign_id, res))
    return {
        'included': res['includedSegments'],
        'excluded': res['excludedSegments']
    }


def update_campaign_segments(cma_url: str, campaign_id: int, included: [int], excluded: [int]) -> None:
    request_url = os.path.join(cma_url, 'campaigns/{0}/segments'.format(campaign_id))
    request_json = {
        "id": int(campaign_id),
        "includedSegmentIds": included,
        "excludedSegmentIds": excluded
    }
    response = requests.put(request_url, json=request_json)

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))
    print('Updated campaign {0} segments: {1}.'.format(campaign_id, request_json))


def camp_segments_list(camp_segments: dict, new_segment_id: Any) -> (List[int], List[int], List[int]):
    included_segments = camp_segments['included']
    excluded_segments = camp_segments['excluded']

    print('>> DEBUG: camp_segments_lists')
    print('included_segments all ids = {0}'.format(','.join(str(x) for x in [s['id'] for s in included_segments])))
    print('excluded_segments all ids = {0}'.format(','.join(str(x) for x in [s['id'] for s in excluded_segments])))

    old_segments = [s for s in included_segments if 'synthetic' in s['description'] and 'lookalike' in s['description']]
    old_segments_ids = [s['id'] for s in old_segments]

    print('current old synthetic segments={0}'.format(','.join(str(x) for x in old_segments_ids)))

    included_segments_ids = [id for id in [s['id'] for s in included_segments] if id not in old_segments_ids]

    print('updated included segments={0}'.format(','.join(str(x) for x in included_segments_ids)))

    if new_segment_id is not None:
        included_segments_ids.append(new_segment_id)
        print('included_segments_ids having new seg')

    excluded_segments_ids = [s['id'] for s in excluded_segments]

    if new_segment_id is None:  # if no new_seg_id => show current As-Is segment info of camp
        print('>> show As-Is')
        return [], included_segments_ids + old_segments_ids, excluded_segments_ids
    else:
        print('>> show new segments updated result')
        return old_segments_ids, included_segments_ids, excluded_segments_ids


def _parse_argv():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--url', help='cma_url')
    parser.add_argument('--bucket', help='file_bucket s3')
    parser.add_argument('--segment', help='campaign_id such as campaign_1234')
    parser.add_argument('--timestamp', help='execution timestamp such as 1999-12-31-17-52-49Z')
    parser.add_argument('--env', help='environment name (stg | prod)')
    parser.add_argument('--jobname', help='job.name in .job file')
    parser.add_argument('--awsregion', help='like ap-south-1')
    args_result = parser.parse_args()

    print(
        'cma_url = {cma}, bucket = {bucket}, segmentId = {seg},  timestamp = {time}, environment ={env}, jobname={jobname}, awsregion={awsregion}'.format(
            cma=args_result.url, bucket=args_result.bucket, seg=args_result.segment, time=args_result.timestamp,
            env=args_result.env, jobname=args_result.jobname, awsregion=args_result.awsregion))

    return args_result


if __name__ == '__main__':
    print('>> update_campaign_segment_started')
    args = _parse_argv()

    batch_name = 'mapfeatures'
    set_datadog_metrics_start(batch_name, args.jobname, args.env)
    table_name = 'map-lookalike-jobs-{0}'.format(args.env)
    aws_region = args.awsregion

    try:

        uuid_str = str(uuid.uuid4())  # random UUID
        camp_id = int(args.segment.replace("campaign_", ""))  # campaign_1234 => 1234 int

        # construct prefix_search and read upload_url from txt file
        prefix_search = os.path.join(args.env, "mapfeatures/look-alike", args.segment, args.timestamp,
                                     args.segment + "_similar_users_scored")
        txt_key = os.path.join(prefix_search, "upload_url.txt")
        upload_url_str = read_body_from_s3(file_bucket=args.bucket, read_key=txt_key, file_type='.txt')
        print('>> this is current content in upload_url.txt = {content}'.format(
            content=upload_url_str))

        # get segment id of csv upload
        new_seg_id = create_synthetic_segment(cma_url=args.url,
                                              segment_name='lookalike_for_{0}_{1}'.format(camp_id, uuid_str),
                                              segment_type='synthetic_lookalike_segment',
                                              uploaded_url=upload_url_str)
        print('>> new seg id = {0}'.format(new_seg_id))

        # camp info

        camp_seg = get_campaign_segments(cma_url=args.url, campaign_id=camp_id)
        existing_synthetic_seg_ids, included_seg_ids, excluded_seg_ids = camp_segments_list(camp_segments=camp_seg,
                                                                                            new_segment_id=new_seg_id)
        print('>> current camp {id} seg info'.format(id=camp_id))
        print(existing_synthetic_seg_ids, included_seg_ids, excluded_seg_ids)

        # update camp seg
        update_campaign_segments(cma_url=args.url, campaign_id=camp_id, included=included_seg_ids,
                                 excluded=excluded_seg_ids)

        # confirm
        time.sleep(30)  # sleep 10 sec
        new_seg_info = get_campaign_segments(cma_url=args.url, campaign_id=camp_id)
        old_synthetic_seg, current_included_seg, current_excluded_seg = camp_segments_list(camp_segments=new_seg_info,
                                                                                           new_segment_id=None)
        print('>> updated As-Is seg info of campaign id = {0}'.format(camp_id))
        print('old synthetic seg = {0}, current included seg = {1}, current excluded seg = {2}'.
              format(','.join(str(x) for x in old_synthetic_seg),
                     ','.join(str(x) for x in current_included_seg),
                     ','.join(str(x) for x in current_excluded_seg)))

        # update status
        table = connect_dynamo(aws_region, table_name)
        id_dict = find_campaign_id(table, args.segment, args.timestamp)
        id_date_utc = id_dict.get("date_utc")
        id_full_name = id_dict.get("id")
        update_status_to_dynamo(table, id_date_utc, id_full_name, 1, 'SUCCEEDED')

        # well completed
        set_datadog_metrics_complete(batch_name, args.jobname, args.env, 0)  # >>> datadog


    except Exception as e:
        set_datadog_metrics_complete(batch_name, args.jobname, args.env, 1)  # >>> datadog
        print("job {1} failed with {0} ".format(e, args.jobname))
        sys.exit(1)
