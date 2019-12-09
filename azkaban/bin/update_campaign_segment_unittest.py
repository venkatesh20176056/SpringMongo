import unittest
from unittest import mock
import os

import requests
import json

import boto3
from moto import mock_s3

from update_campaign_segment import *


# for mock-API
def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'cma_url/segments' and \
            kwargs.get('json').get('csvUrl') is not None and \
            kwargs.get('json').get('description') is not None and \
            kwargs.get('json').get('name') is not None and \
            kwargs.get('json').get('type') is not None:
        return MockResponse({"id": 123456789}, 200)
    # need more, add here

    return MockResponse(None, 400)  # bad response


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'cma_url/campaigns/123456789':
        return MockResponse({"includedSegments": [1111, 2222], "excludedSegments": [9999]}, 200)
    # need more, add here

    return MockResponse(None, 400)  # bad response


def mocked_requests_put(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code, text):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data

        def text(self):
            return self.text

    if args[0] == 'cma_url/campaigns/123456789/segments':
        return MockResponse({"id": 123456789}, 200, 'ok')
    # need more, add here

    return MockResponse(None, 400, 'failed')  # bad response


class upload_csv_from_s3_to_cma_unittest(unittest.TestCase):

    @mock.patch('requests.post', side_effect=mocked_requests_post)
    def test_create_synthetic_segment(self, mock_post):
        actual = create_synthetic_segment(cma_url='cma_url', segment_name='test_seg', segment_type='test_type',
                                          uploaded_url='upload_url')
        expected = 123456789
        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))
        self.assertEqual(actual, expected)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_campaign_segments(self, mock_get):
        call = get_campaign_segments(cma_url='cma_url', campaign_id=123456789)
        actual = [call.get('included'), call.get('excluded')]
        expected = [[1111, 2222], [9999]]
        print(sys._getframe().f_code.co_name + ':  actual = {act}, expected = {exp}'.format(act=actual, exp=expected))
        self.assertEqual(actual, expected)

    @mock.patch('requests.put', side_effect=mocked_requests_put)
    def test_update_campaign_segments(self, mock_put):
        new_seg = 3333
        call_get ={"included":[1111,2222], "excluded":[9999]}

        actual = update_campaign_segments(cma_url='cma_url', campaign_id=123456789,
                                 included=call_get.get('included').append(new_seg),
                                 excluded=call_get.get('excluded'))
        self.assertIsNone(actual)
