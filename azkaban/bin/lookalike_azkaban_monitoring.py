import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict
import re
import requests
from requests.exceptions import HTTPError
import boto3
import argparse
import sys
import xml.etree.ElementTree as ET
import pytz

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def process_date_str(x, pattern): return datetime.fromtimestamp((x / 1000.0), tz=pytz.utc).strftime(pattern)


def process_date(x): return datetime.fromtimestamp((x / 1000.0), tz=pytz.utc)


class AzkabanWorkFlow(object):

    def __init__(self, url: str, username: str, pwd: str):
        self.session = None
        self.url = url
        self.username = username
        self.pwd = pwd
        self.result = None
        self.__authenticate()

    def __authenticate(self):
        data = {
            'action': 'login',
            'username': self.username,
            'password': self.pwd
        }
        try:
            response = requests.post(self.url, data=data)
            response.raise_for_status()
        except HTTPError as err:
            print(err.response)
            sys.exit(1)

        self.session = response.json()['session.id']

    def __process_azk_result(self, executions: List[dict], job_id: str) -> List[dict]:
        """
        Processes the result for persist
        :param executions: All azkaban executions
        :param job_id: job_id to get azkaban logs
        :return:
        """
        out_list = []
        for i in executions:
            resp = self.get_job_logs(i['execId'], job_id, offset=0, limit=10000)
            log_data = resp['data']
            result = re.findall(r'campaign_(\d+)', log_data)
            campaign_name = result[0] if result else None
            if campaign_name:
                end_time = process_date(i['endTime'])
                start_time = process_date(i['startTime'])
                run_duration = (end_time - start_time).total_seconds()
                run_time = divmod(run_duration, 60)[0]
                exec_dict = {
                    'id': campaign_name,
                    'submitTime': process_date_str(i['submitTime'], '%Y-%m-%d %H:%M:%S'),
                    'startTime': process_date_str(i['startTime'], '%Y-%m-%d %H:%M:%S'),
                    'endTime': process_date_str(i['endTime'], '%Y-%m-%d %H:%M:%S'),
                    'status': i['status'],
                    'execId': i['execId'],
                    'runTime(mins)': run_time
                }
                out_list.append(exec_dict)
        return out_list

    def __get_azk_flow_executions(self, project, flow, start: int, end: int, target_date: datetime, exec_list: list):
        """
        Util function that recursively gets the execution flow
        :param project: Azkaban project
        :param flow: Azkaban flow
        :param start: start index of buffer
        :param end: end index of buffer
        :param target_date: end date
        :param exec_list: execution flow list
        :return:
        """
        resp = self.get_workflow_executions(project, flow, start, end)['executions']
        job_datetime = process_date_str(resp[end - 1]['submitTime'], '%Y-%m-%d %H:%M')
        job_date = datetime.strptime(job_datetime, '%Y-%m-%d %H:%M')
        if job_date >= target_date:
            exec_list += [i for i in resp]
            self.__get_azk_flow_executions(project, flow, end + 1, end + 10, target_date, exec_list)
        else:
            exec_list += [i for i in resp]
            i = len(exec_list) - 1
            while exec_list:
                job_datetime = process_date_str(exec_list[i]['submitTime'], '%Y-%m-%d %H:%M')
                job_date = datetime.strptime(job_datetime, '%Y-%m-%d %H:%M')
                if job_date < target_date:
                    exec_list.pop()
                    i -= 1
                elif job_date >= target_date:
                    break

            return

    def get_workflow_executions(self, project: str, flow: str, start: int, end: int) -> Dict:
        data = {
            'ajax': 'fetchFlowExecutions',
            'session.id': self.session,
            'project': project,
            'flow': flow,
            'start': start,
            'length': end
        }
        url = '{url}/manager'.format(url=self.url)
        try:
            response = requests.get(url, data)
            response.raise_for_status()
        except HTTPError as err:
            print(err.response)
            sys.exit(1)
        return response.json()

    def get_job_logs(self, exec_id: str, job_id: str, offset: int, limit: int) -> Dict:
        data = {
            'ajax': 'fetchExecJobLogs',
            'session.id': self.session,
            'execid': exec_id,
            'jobId': job_id,
            'offset': offset,
            'length': limit
        }
        url = '{url}/executor'.format(url=self.url)
        try:
            response = requests.get(url, data)
            response.raise_for_status()
        except HTTPError as err:
            print(err.response)
            sys.exit(1)
        return response.json()

    def get_azk_daily_executions(self, end_date_str: str, project: str, flow: str, job_id: str) -> List[dict]:
        """
        Gets all the Azkaban flow executions ending at end_date_str param
        :param end_date_str: end date up to which the flow executions are gathered
        :param project: Azkaban project
        :param flow: Azkaban flow
        :param job_id: Azkaban job_id
        :return: Flow executions as a json
        """
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M')
        execution_lst = []
        self.__get_azk_flow_executions(project, flow, 0, 10, end_date, execution_lst)
        return self.__process_azk_result(execution_lst, job_id)


if __name__ == '__main__':
    _log.info('Begin Execution')

    parser = argparse.ArgumentParser(description='Azkaban execution parser')
    parser.add_argument('-f', '--flow', type=str, help='Azkaban flow')
    parser.add_argument('-p', '--project', type=str, help='Azkaban project')
    parser.add_argument('--azk_url', type=str, help='Azkaban url')
    parser.add_argument('-j', '--job_id', type=str, help='Azkaban job id')
    parser.add_argument('-b', '--midgarBucket', type=str, help='S3 Bucket')
    parser.add_argument('--path', type=str, help='S3 file path')
    parser.add_argument('--cred_path', type=str, help='azk credentials path')

    args = parser.parse_args()

    date = (datetime.now(pytz.utc)-timedelta(1)).strftime('%Y-%m-%d %H:%M')

    file_date = datetime.now(pytz.utc).strftime('%Y-%m-%d')

    print("Target end date:{d}".format(d=date))

    s3_file_path = '{path}/{date}/lookalike_azkaban_info.json'.format(path=args.path, date=file_date)

    cred_map = ET.parse(args.cred_path).getroot().find('user').attrib
    azk_workflow = AzkabanWorkFlow(url=args.azk_url, username=cred_map['username'], pwd=cred_map['password'])
    azk_result = azk_workflow.get_azk_daily_executions(end_date_str=date, project=args.project,
                                                       flow=args.flow, job_id=args.job_id)

    print(json.dumps(azk_result, indent=2))

    s3 = boto3.resource('s3', region_name='ap-south-1')
    s3Object = s3.Object(args.midgarBucket, s3_file_path)
    s3Object.put(
        Body=(bytes(json.dumps(azk_result).encode('UTF-8')))
    )

    _log.info('End of Execution')
