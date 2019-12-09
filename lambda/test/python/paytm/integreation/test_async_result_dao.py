import unittest
from uuid import uuid4

import boto3
from paytm.async_result.async_result_dao import AsyncResultDao

_endpoint_url = 'http://localhost:8000'
_table_name = 'TestAsyncResultDao'


class TestAsyncResultDao(unittest.TestCase):

    def test_persist_load(self):
        id = str(uuid4())
        expected = 'http://example.com/{0}'.format(uuid4())
        self.dao.persist(id, expected)
        result = self.dao.load(id)
        self.assertEqual(result, expected)

    def test_remove(self):
        id = str(uuid4())
        data = 'http://example.com/{0}'.format(uuid4())
        self.dao.persist(id, data)
        self.assertTrue(self.dao.load(id) is not None)
        self.dao.remove(id)
        self.assertTrue(self.dao.load(id) is None)

    def setUp(self):
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=_endpoint_url)
        self._delete_table()
        self._create_table()
        self.dao = AsyncResultDao(aws_region='local', table_name=_table_name, endpoint_url=_endpoint_url)

    def tearDown(self):
        self._delete_table()

    def _create_table(self):
        self.dynamodb.create_table(
            TableName=_table_name,
            KeySchema=[
                { 'AttributeName': 'id', 'KeyType': 'HASH' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'id', 'AttributeType': 'S' }
            ],
            ProvisionedThroughput={ 'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10 }
        )

    def _delete_table(self):
        try:
            self.dynamodb.Table(_table_name).delete()
        except Exception:
            pass
