from datetime import datetime, timedelta, timezone
import unittest

import boto3

from paytm.job import Status
from paytm.job.job_dao import JobDao
from paytm.job.stub_factory import mk_job
from paytm.time.time import format_date

_endpoint_url = 'http://localhost:8000'
_table_name = 'TestJobDao'


class TestJobDao(unittest.TestCase):

    def test_persist_load(self):
        now = datetime.now(timezone.utc) - timedelta(days=10)
        expected = mk_job(1, now)

        self.dao.persist(expected)
        job = self.dao.load(format_date(now), expected.id)

        self.assertEqual(job, expected)

    def test_list_unfinished(self):
        now = datetime.now(timezone.utc) - timedelta(days=20)

        successful_job = mk_job(1, now)
        successful_job.status = Status.SUCCEEDED
        failed_job = mk_job(2, now)
        failed_job.status = Status.FAILED
        unfinished_job = mk_job(3, now)

        self.dao.persist(successful_job)
        self.dao.persist(failed_job)
        self.dao.persist(unfinished_job)

        jobs = self.dao.list_unfinished(now)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0], unfinished_job)

    def test_list_unfinished_window(self):
        now = datetime.now(timezone.utc)

        future_job = mk_job(1, now + timedelta(seconds=1))
        current_job1 = mk_job(2, now)
        current_job2 = mk_job(3, now - timedelta(days=1) + timedelta(seconds=1))
        past_job = mk_job(4, now - timedelta(days=1))

        self.dao.persist(future_job)
        self.dao.persist(current_job1)
        self.dao.persist(current_job2)
        self.dao.persist(past_job)

        jobs = self.dao.list_unfinished(now)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0], current_job2)
        self.assertEqual(jobs[1], current_job1)

    def setUp(self):
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=_endpoint_url)
        self._delete_table()
        self._create_table()
        self.dao = JobDao(aws_region='local', table_name=_table_name, endpoint_url=_endpoint_url)

    def tearDown(self):
        self._delete_table()

    def _create_table(self):
        self.dynamodb.create_table(
            TableName=_table_name,
            KeySchema=[
                { 'AttributeName': 'date_utc', 'KeyType': 'HASH' },
                { 'AttributeName': 'id', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'date_utc', 'AttributeType': 'S' },
                { 'AttributeName': 'id',  'AttributeType': 'S' },
            ],
            ProvisionedThroughput={ 'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10 }
        )

    def _delete_table(self):
        try:
            self.dynamodb.Table(_table_name).delete()
        except Exception:
            pass
