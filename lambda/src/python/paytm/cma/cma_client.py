from datetime import date
import logging
from typing import Any, List, Optional
from paytm.look_alike import env_vars


import requests
from requests import HTTPError

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


class CmaClient:

    def __init__(self, service_url: str, tenant: str) -> None:
        """
        :param service_url: Example: 'http://{host}'
        """
        base_url = service_url
        if service_url.endswith('/'):
            base_url = service_url[:-1]
        self.url = '{base_url}/clients/{tenant}'.format(base_url=base_url, tenant=tenant)
        self.tenant = tenant

    def get_scheduled_campaign(self, tenant_date: date, campaign_id: int) -> Optional[dict]:
        date_str = tenant_date.strftime('%Y-%m-%d')
        url_template = self.url + '/management/executions/{date}/campaigns/{id}'
        request_url = url_template.format(date=date_str, id=campaign_id)
        response = requests.get(request_url)

        if response.status_code == 204:
            _log.warning('Campaign {0} returns status code 204 when hitting {1}'.format(campaign_id, request_url))
            return None

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        try:
            res = response.json()
            _log.info('Fetched campaign: {0}'.format(res))
            return res
        except Exception as e:
            _log.error('Request to {0} returned bad JSON: {1}'.format(request_url, response.text))
            raise e

    def get_campaign_details(self, campaign_id: int) -> Optional[dict]:
        url_template = self.url + '/campaigns/{id}'
        request_url = url_template.format(id=campaign_id)
        response = requests.get(request_url)

        if response.status_code == 204:
            _log.warning('Campaign {0} returns status code 204 when hitting {1}'.format(campaign_id, request_url))
            return {}

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        try:
            res = response.json()
            _log.info('Fetched campaign details: {0}'.format(res))
            return res
        except Exception as e:
            _log.error('Request to {0} returned bad JSON: {1}'.format(request_url, response.text))
            raise e

    def get_campaigns_with_boost(self) -> List[dict]:
        request_url = self.url + '/campaigns?hasAudienceBoost=true&state=created,started'
        response = requests.get(request_url)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        try:
            res = response.json()
            campaign_ids = [campaign['id'] for campaign in res]
            _log.info('Fetched campaigns with boost: {0}.'.format(campaign_ids))
            return res
        except Exception as e:
            _log.error('Request to {0} returned bad JSON: {1}'.format(request_url, response.text))
            raise e

    def get_all_scheduled_campaigns(self, tenant_date: date, query_str: str) -> List[dict]:
        date_str = tenant_date.strftime('%Y-%m-%d')
        url_template = self.url + '/management/executions/{date}?{query_str}'
        request_url = url_template.format(date=date_str, query_str=query_str)
        response = requests.get(request_url)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        try:
            res = response.json()
            campaign_ids = [campaign['id'] for campaign in res]
            _log.info('Fetched all campaigns: {0}.'.format(campaign_ids))
            return res
        except Exception as e:
            _log.error('Request to {0} returned bad JSON: {1}'.format(request_url, response.text))
            raise e

    def upload_segment_data(self, file_obj: Any) -> str:
        request_url = self.url + '/files/upload'
        multipart_data = {
            'file': file_obj,
            'uploadType': 'segment'
        }
        response = requests.post(request_url, files=multipart_data)
        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        return response.json()['url']

    def create_synthetic_segment(self, segment_name: str, segment_type: str, uploaded_url: str) -> int:
        request_url = self.url + '/segments'
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
        _log.info('Created synthetic segment: {0}.'.format(res))
        return res['id']

    def get_campaign_segments(self, campaign_id: int) -> dict:
        request_url = self.url + '/campaigns/{0}'.format(campaign_id)
        response = requests.get(request_url)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))

        res = response.json()
        _log.info('Loaded campaign {0} segments: {1}.'.format(campaign_id, res))
        return {
            'included': res['includedSegments'],
            'excluded': res['excludedSegments']
        }

    def update_campaign_segments(self, campaign_id: int, included: List[int], excluded: List[int]) -> None:
        request_url = self.url + '/campaigns/{0}/segments'.format(campaign_id)
        request_json = {
            "id": int(campaign_id),
            "includedSegmentIds": included,
            "excludedSegmentIds": excluded
        }
        response = requests.put(request_url, json=request_json)

        if response.status_code < 200 or response.status_code >= 300:
            raise HTTPError('Status code {0}; {1}'.format(response.status_code, response.text))
        _log.info('Updated campaign {0} segments: {1}.'.format(campaign_id, request_json))

    def delete_segment(self, segment_id: int) -> None:
        # No CMA endpoint yet.
        pass
