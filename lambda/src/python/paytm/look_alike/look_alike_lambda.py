import json
import logging
from typing import Any, Dict

from paytm.look_alike import campaign_daily_handler, campaign_push_handler, job_queue_handler
from paytm.look_alike.env_vars import AWS_REGION
from paytm.ssm.oauth import get_oauth_token

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], _) -> dict:
    # this main function when AWS lambda get triggered.
    # see => _handle_request(body)
    # event :Dict has camp_id and trigger type info

    if not _is_authorized(event):
        _log.warning('Got event with invalid token.')
        return _unauthorized_request("Invalid token.")

    if 'body' in event:
        body = json.loads(event['body'])
        _log.info('Got event body {0}.'.format(body))
        if 'trigger' in body:
            return _handle_request(body)
        else:
            return _failed_execution('Missing trigger.')
    else:
        return _failed_execution('Missing body.')


def _is_authorized(event: Dict[str, Any]) -> bool:
    if 'headers' in event and 'Authorization' in event['headers']:
        token = get_oauth_token(AWS_REGION, '/map-bpplus/production')
        if not token:
            _log.error('Could not get oath token.')
            return False
        if event['headers']['Authorization'] == token:
            return True
    return False


def _handle_request(request) -> dict:

    trigger = request['trigger']

    _log.info("below: body of request at _handle_request ")

    # depending on type of trigger, different handling flow
    # FYI: push and daily => create job info in dynamo DB only , NOT conducting spark-job submit
    # FYI: only periodic => triggered by cloudWatch (i: every 6 minutes, ii: every day) => see job info from dynamo DB
    #       => if some jobs not finished , submit spark-job

    # cloudWatch generates 2 types of event => run lambda functions
    # daily => place long task list per day
    # push => one or few items per each

    # once tasks updated, periodic will process the tasks accordingly.

    if trigger == 'push':
        campaign_push_handler.boost_campaign(request['campaign_id'])
    elif trigger == 'daily':
        campaign_daily_handler.start_daily_dataset_update()
    elif trigger == 'periodic':
        job_queue_handler.drive_job_queue()
    else:
        return _failed_execution('Trigger must be push|daily|periodic.')
    return _successful_execution('OK')


def _successful_execution(msg):
    return {'statusCode': 200, 'body': json.dumps(msg)}


def _failed_execution(msg):
    return {'statusCode': 400, 'body': json.dumps(msg)}


def _unauthorized_request(msg):
    return {'statusCode': 401, 'body': json.dumps(msg)}
