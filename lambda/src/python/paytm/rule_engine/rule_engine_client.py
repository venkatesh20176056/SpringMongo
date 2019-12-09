import logging

import requests
from requests import HTTPError

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


class RuleEngineClient:

    def __init__(self, url: str, tenant: str, client_id: str) -> None:
        """
        :param url: 'http://map-rule-engine-production.paytm.com'
        :param tenant: 'paytm-canada' | 'paytm-india'
        """
        if url.endswith('/'):
            self.url = url[:-1]
        else:
            self.url = url
        self.tenant = tenant
        self.client_id = client_id
        self.headers = {
            'x-request-client': self.client_id,
            'tenant': self.tenant
        }

    def create_dataset(self, campaign_id: int, inclusion: dict, exclusion: dict, fields: list) -> str:
        request_url = self.url + '/v1/execute'
        rules = {
            'campaign_id': campaign_id,
            'inclusion': inclusion,
            'exclusion': exclusion,
            'fields': fields
        }
        response = requests.post(request_url, headers=self.headers, json=rules)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        response_json = response.json()
        _log.info('Create dataset for campaign {0} returned {1}.'.format(campaign_id, response_json))

        if 'request_id' in response_json:
            return response_json['request_id']
        else:
            raise Exception('Failed to generate dataset for campaign: {0}.'.format(campaign_id))

    def get_dataset_status(self, request_id: str) -> dict:
        request_url = self.url + '/v1/request/{request_id}/status'.format(request_id=request_id)
        response = requests.get(request_url, headers=self.headers)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        return response.json()
