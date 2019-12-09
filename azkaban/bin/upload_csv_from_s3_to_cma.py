import logging
import sys
from typing import Any
import argparse
import os

import boto3
import requests
from requests import HTTPError
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete

log = logging.getLogger('upload_csv_to_cma')

'''
# flow logic:
# 1. previous Azkaban job shares its segmentID (campaign id) and executionTimestamp with this flow
# 2. with the given info above, search relevant csv file key
# 3. upload csv file from s3 to CMA and receive upload_url
# 4. upload this upload_url to the same s3 directory as txt file
'''


def filter_key_name(contents: dict, search_word: str) -> [str]:
    filtered_search_word = filter(lambda x: search_word in x.get('Key'), contents)
    filtered_key = list(map(lambda x: x.get('Key'), filtered_search_word))

    return filtered_key


def get_content_from_s3(file_bucket: str, prefix_search: str) -> dict:
    s3 = boto3.client('s3')
    file_dict = s3.list_objects_v2(Bucket=file_bucket, Prefix=prefix_search)
    all_contents = file_dict.get('Contents')

    return all_contents


def remove_existing_file_from_s3(file_bucket: str, remove_key: str) -> int:
    s3obj = boto3.resource('s3')
    s3obj.Object(file_bucket, remove_key).delete()

    bucket = s3obj.Bucket(file_bucket)
    objs = list(bucket.objects.filter(Prefix=remove_key))

    return len(objs)  # not exist ->return 0


def write_to_s3(file_bucket: str, write_key: str, body: Any) -> None:
    s3obj = boto3.resource('s3')
    s3obj.Object(file_bucket, write_key).put(Body=body)


def find_first_key(file_bucket: str, prefix_search: str, search_word: str) -> str:
    all_contents = get_content_from_s3(file_bucket=file_bucket, prefix_search=prefix_search)

    target_csv_key_list = filter_key_name(contents=all_contents, search_word=search_word)

    target_csv_key = None if not target_csv_key_list else target_csv_key_list[0]  # return the first one always

    if target_csv_key is None:
        print(">>>> No key found, check if file (csv or txt) exists or not")
        raise TypeError

    else:
        return target_csv_key


def read_body_from_s3(file_bucket: str, read_key: str, file_type: str) -> str:
    s3obj = boto3.resource('s3')

    key_found = find_first_key(file_bucket=file_bucket, prefix_search=read_key, search_word=file_type)

    body_content = s3obj.Object(file_bucket, key_found).get()['Body'].read()

    return body_content.decode('utf8')


def upload_file_to_cma(cma_url: str, file_bucket: str, file_key: str) -> str:
    s3 = boto3.client('s3')

    file_obj = s3.get_object(Bucket=file_bucket, Key=file_key)['Body']

    print('Uploading file to CMA...')

    upload_url = upload_segment_data(cma_url, file_obj)

    print('upload_url is {upload_url}'.format(upload_url=upload_url))

    print('Finished successfully.')

    return upload_url


def upload_segment_data(cma_url: str, file_obj: Any) -> str:

    request_url = os.path.join(cma_url, 'files/upload')

    print('request_url={0}'.format(request_url))

    print('file_obj below:')
    print(file_obj)


    multipart_data = {
        'file': file_obj,
        'uploadType': 'segment'
    }
    response = requests.post(request_url, files=multipart_data)

    print('upload response code = {res}'.format(res=response.status_code))

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))
    return response.json()['url']


def _parse_argv():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--url', help='cma_url')
    parser.add_argument('--bucket', help='file_bucket s3')
    parser.add_argument('--segment', help='campaign_id such as campaign_1234')
    parser.add_argument('--timestamp', help='execution timestamp such as 1999-12-31-17-52-49Z')
    parser.add_argument('--env', help='environment name (stg | prod)')
    parser.add_argument('--jobname', help='job.name in .job file')
    args_result = parser.parse_args()

    print('cma_url = {cma}, bucket = {bucket}, segmentId = {seg},  timestamp = {time}, environment ={env}, jobname={jobname}'.format(
        cma=args_result.url, bucket=args_result.bucket, seg=args_result.segment, time=args_result.timestamp,
        env=args_result.env, jobname = args_result.jobname))

    return args_result


if __name__ == '__main__':
    print('upload_csv_from_s3_to_cma started')
    args = _parse_argv()
    batch_name = 'mapfeatures'

    set_datadog_metrics_start(batch_name, args.jobname, args.env) # >>> datadog

    try:

        # construct prefix_search
        prefix_search = os.path.join(args.env, "mapfeatures/look-alike", args.segment, args.timestamp,
                                     args.segment + "_similar_users_scored")

        csv_file_key = find_first_key(file_bucket=args.bucket, prefix_search=prefix_search, search_word='.csv')
        print('csv_file_key = {0}'.format(csv_file_key))

        # get upload_url
        upload_url = upload_file_to_cma(cma_url=args.url, file_bucket=args.bucket,
                                        file_key=csv_file_key)
        print('csv file uploaded from {here} to {there}'.format(here=csv_file_key, there=upload_url))
        txt_key = os.path.join(prefix_search , "upload_url.txt")
        remove_existing_file_from_s3(file_bucket=args.bucket, remove_key=txt_key)

        # upload_url saved to txt file in same directory
        write_to_s3(file_bucket=args.bucket, write_key=txt_key, body=upload_url)
        print('upload_url.txt is saved, it has actual upload_url where csv file resides in CMA')
        print(' this is current content in upload_url.txt = {content}'.format(
            content=read_body_from_s3(file_bucket=args.bucket, read_key=txt_key, file_type='.txt')))

        set_datadog_metrics_complete(batch_name, args.jobname, args.env, 0) # >>> datadog

    except Exception as e:
        set_datadog_metrics_complete(batch_name, args.jobname, args.env, 1) # >>> datadog
        print("job {1} failed with {0} ".format(e, args.jobname))
        sys.exit(1)
