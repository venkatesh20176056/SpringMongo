import pkg_resources
import time
import boto3
import json
import logging
import os
from enum import Enum
from typing import Dict, Optional


DEBUG_MODE = os.environ.get('DEBUG_MODE') in ['true']


log = logging.getLogger('paytm.crawler')
glue = boto3.client('glue')


class State(Enum):
    READY = 1
    RUNNING = 2
    STOPPING = 3


def load_template(name: str) -> str:
    resource_path = '{name}.json'.format(name=name)
    bytes = pkg_resources.resource_string(__name__, resource_path)
    return bytes.decode("UTF-8")


def substibute(template: str, **replacements) -> str:
    res = template
    for key, value in replacements.items():
        res = res.replace('${' + key + '}', value)
    return res


def run_crawler(name: str, crawler_json: str):
    log.info('Starting to run crawler {name}.'.format(name=name))
    ensure_deleted(name)
    assert_crawler_target_exists(crawler_json)
    remote_create(name, crawler_json)
    remote_start(name)
    wait_for_completion(name, poll_wait=20, max_wait=4 * 3600)
    if not DEBUG_MODE:
        remote_delete(name)
    log.info('Finished running crawler {name}.'.format(name=name))


def assert_crawler_target_exists(crawler_json: str):
    props = json.loads(crawler_json)
    for target in props['Targets']['S3Targets']:
        target_url = target['Path']
        assert_s3_dir_exists(target_url)


def assert_s3_dir_exists(url: str):
    s3 = boto3.client('s3')
    prefix = 's3://'
    assert url[:len(prefix)] == prefix
    bucket_path = url[len(prefix):]
    slash_index = bucket_path.index('/')
    bucket = bucket_path[:slash_index]
    path = bucket_path[slash_index + 1:]
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=path,
    )
    assert response['KeyCount'] > 0


def ensure_deleted(name: str):
    status = remote_status(name)
    if status is not None:
        if status == State.READY:
            remote_delete(name)
            assert remote_status(name) == None
        else:
            raise AssertionError('Crawler {name} is in a wrong state.'.format(name=name))


def wait_for_completion(name: str, poll_wait: float, max_wait: float):
    log.info('Awaiting completion for crawler {name}.'.format(name=name))
    done = False
    start_time = time.time()
    while not done:
        time.sleep(poll_wait)
        status = remote_status(name)
        assert status != None
        if status == State.READY:
            done = True
        elif time.time() - start_time > max_wait:
            raise TimeoutError('Timed out waiting for crawler {name} to finish.'.format(name=name))


def remote_status(name: str) -> Optional[State]:
    log.info('Polling status of remote crawler {name}.'.format(name=name))
    try:
        response = glue.get_crawler(Name=name)
        state = response['Crawler']['State']
        return State[state]
    except glue.exceptions.EntityNotFoundException:
        return None


def remote_create(name: str, crawler_json: str):
    log.info('Creating remote crawler {name}.'.format(name=name))
    props = json.loads(crawler_json)
    glue.create_crawler(**props)


def remote_start(name: str):
    log.info('Starting remote crawler {name}.'.format(name=name))
    glue.start_crawler(Name=name)


def remote_delete(name: str):
    log.info('Deleting remote crawler {name}.'.format(name=name))
    glue.delete_crawler(Name=name)
