from datetime import datetime, timezone
import logging
from typing import Optional, Any

import boto3

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


class AsyncResultDao:

    def __init__(self, aws_region: str, table_name: str, endpoint_url: Optional[str] = None) -> None:
        self.aws_region = aws_region
        self.table_name = table_name

        if endpoint_url:
            self._dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)
        else:
            self._dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        self._table = self._dynamodb.Table(table_name)

    def persist(self, result_id: str, result: Any) -> None:
        item = {
            'id': result_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'result': result
        }
        _log.info('Persisting result {0}.'.format(item))
        self._table.put_item(Item=item)

    def load(self, result_id: str) -> Optional[Any]:
        response = self._table.get_item(
            Key={
                'id': result_id
            }
        )
        if 'Item' in response:
            return response['Item']['result']
        else:
            return None

    def remove(self, result_id: str) -> None:
        key = {
            'id': result_id
        }
        _log.info('Deleting result {0}.'.format(key))
        self._table.delete_item(Key=key)
