import unittest
from unittest import mock
import os
import sys

import requests
import json

import boto3
from moto import mock_s3

from upload_csv_from_s3_to_cma import *


# for mock-API
def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'cma_url/files/upload':
        return MockResponse({"url": "upload_url_response"}, 200)
    # need more, add here

    return MockResponse(None, 400)  # bad response


class upload_csv_from_s3_to_cma_unittest(unittest.TestCase):
    '''

    1. check install moto for AWS unit test -either pip or conda
    2. directory setup in order to import your testing script

    '''

    # test cases:
    # 1. filter_key_name(key_str, search_word) -> [key]
    # 2. read_file_keys(bucket, prefix) -> [key]
    # 3. _upload_segment_data(cma_url, file_obj) -> upload_url:str
    # 4. write_upload_url_to_s3(upload_url, bucket, prefix) -> None
    # 5. remove_existing_from_s3(remove_file_name, bucket, prefix) -> None

    def test_filter_key_name(self):
        content_json_str = """{
                                'IsTruncated': True|False,
                                'Contents': [
                                    {
                                        'Key': 'string.csv',
                                        'LastModified': 'datetime(2015, 1, 1)',
                                        'ETag': 'string',
                                        'Size': 123,
                                        'StorageClass': 'STANDARD',
                                        'Owner': {
                                            'DisplayName': 'string',
                                            'ID': 'string'
                                        }
                                    },
                                            {
                                        'Key': 'string.txt',
                                        'LastModified': 'datetime(2015, 1, 1)',
                                        'ETag': 'string',
                                        'Size': 123,
                                        'StorageClass': 'STANDARD',
                                        'Owner': {
                                            'DisplayName': 'string',
                                            'ID': 'string'
                                        }
                                    },
                                            {
                                        'Key': '_SUCCESS',
                                        'LastModified': 'datetime(2015, 1, 1)',
                                        'ETag': 'string',
                                        'Size': 123,
                                        'StorageClass': 'STANDARD',
                                        'Owner': {
                                            'DisplayName': 'string',
                                            'ID': 'string'
                                        }
                                    }
                                ],
                                'Name': 'string',
                                'Prefix': 'string',
                                'Delimiter': 'string',
                                'MaxKeys': 123,
                                'CommonPrefixes': [
                                    {
                                        'Prefix': 'string'
                                    }
                                ],
                                'EncodingType': 'url',
                                'KeyCount': 123,
                                'ContinuationToken': 'string',
                                'NextContinuationToken': 'string',
                                'StartAfter': 'string'
                            }"""

        content_json_str_formatted = content_json_str.replace("'", '"').replace('\n', '')
        content_dict = eval(content_json_str_formatted)
        search_word = '.csv'

        expected = ['string.csv']
        actual = filter_key_name(content_dict.get('Contents'), search_word)

        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))

        self.assertEqual(actual, expected)

    @mock_s3
    def test_get_content_from_s3(self):
        test_region = 'us-east-1'
        conn = boto3.resource('s3', region_name=test_region)  # set up connection
        conn.create_bucket(Bucket='testbucket')  # create bucket initially

        prefix_str = 'stg/camp_100/9999-12-31 12:12:12Z'

        client = boto3.client('s3', region_name=test_region)
        client.put_object(Bucket='testbucket', Key=os.path.join(prefix_str, 'lookalike.csv'), Body='abc')

        expected = os.path.join(prefix_str, 'lookalike.csv')
        actual = get_content_from_s3(file_bucket='testbucket', prefix_search=prefix_str)[0].get('Key')
        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))
        self.assertEqual(actual, expected)

    @mock_s3
    def test_remove_existing_file_from_s3(self):
        test_region = 'us-east-1'
        conn = boto3.resource('s3', region_name=test_region)  # set up connection
        conn.create_bucket(Bucket='testbucket')  # create bucket initially

        # no existing file

        prefix_str = 'stg/camp_100/9999-12-31 12:12:12Z'
        print(remove_existing_file_from_s3(file_bucket='testbucket',
                                           remove_key=os.path.join(prefix_str, 'lookalike.txt')))

        client = boto3.client('s3', region_name=test_region)
        client.put_object(Bucket='testbucket', Key=os.path.join(prefix_str, 'lookalike.txt'), Body='abc')

        expected = 0

        actual = remove_existing_file_from_s3(file_bucket='testbucket', remove_key=os.path.join(prefix_str,
                                                                                                'lookalike.txt'))
        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))
        self.assertEqual(expected, actual)

    @mock_s3
    def test_find_csv_key(self):
        test_region = 'us-east-1'
        conn = boto3.resource('s3', region_name=test_region)  # set up connection
        conn.create_bucket(Bucket='testbucket')  # create bucket initially

        prefix_str = 'stg/camp_100/9999-12-31 12:12:12Z'

        client = boto3.client('s3', region_name=test_region)
        client.put_object(Bucket='testbucket', Key=os.path.join(prefix_str, 'lookalike.csv'), Body='abc')

        print(client.list_objects_v2(Bucket="testbucket", Prefix=prefix_str))

        print(get_content_from_s3(file_bucket="testbucket", prefix_search=prefix_str))

        expected = os.path.join(prefix_str, 'lookalike.csv')
        actual = find_first_key(file_bucket='testbucket', prefix_search=prefix_str, search_word='.csv')

        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))

        self.assertEqual(actual, expected)

        # actual_error = find_first_key(file_bucket='testbucket', prefix_search=prefix_str, search_word='.error')
        self.assertRaises(TypeError, find_first_key, file_bucket='testbucket', prefix_search=prefix_str, search_word='.error')

    @mock_s3
    def test_write_to_s3(self):
        test_region = 'us-east-1'
        conn = boto3.resource('s3', region_name=test_region)  # set up connection
        conn.create_bucket(Bucket='testbucket')  # create bucket initially

        prefix_str = 'stg/camp_100/9999-12-31 12:12:12Z'

        write_to_s3(file_bucket='testbucket', write_key=os.path.join(prefix_str, "upload_url.txt"), body="abc")

        actual = find_first_key(file_bucket="testbucket", prefix_search=prefix_str, search_word='.txt')
        expected = os.path.join(prefix_str, "upload_url.txt")

        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))
        self.assertEqual(actual, expected)

    @mock.patch('requests.post', side_effect=mocked_requests_post)
    def test_upload_segment_data(self, mock_post):
        actual = upload_segment_data(cma_url='cma_url', file_obj='abc')
        expected = 'upload_url_response'

        self.assertEqual(actual, expected)
