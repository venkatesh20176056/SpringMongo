import boto3
import logging
from typing import List


DATE_LEN = 10 # Number of date characters, for example 2018_08_10 -> 10 characters.
MAX_LINES = 100
FILE_BUCKET = 'midgar-aws-workspace'


log = logging.getLogger('paytm.table_alias_file')
s3 = boto3.client('s3')


def update(file_key: str, alias: str):
    log.info('Starting file {file_key} update with {alias}.'.format(file_key=file_key, alias=alias))
    contents = load(file_key)
    contents = update_contents(alias, contents)
    save(file_key, contents)
    log.info('Finished file {file_key} update with {alias}.'.format(file_key=file_key, alias=alias))


def update_contents(alias: str, contents: str) -> str:
    log.info('Updaging file contents with {alias}.'.format(alias=alias))
    lines = contents.split('\n')
    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line]
    assert lines
    _update_lines(alias, DATE_LEN, lines)
    lines = lines[:MAX_LINES]
    lines.append('')
    return '\n'.join(lines)


def _update_lines(alias: str, date_len: int, lines: List[str]):
    raw_aliases = lines[0].split(',')
    aliases = [alias.strip() for alias in raw_aliases]
    prefixes = [alias[:-date_len] for alias in aliases]
    target_prefix = alias[:-date_len]
    target_index = prefixes.index(target_prefix)
    aliases[target_index] = alias
    new_line = ','.join(aliases)
    if new_line != lines[0]:
        lines.insert(0, new_line)


def load(key: str) -> str:
    log.info('Loading file {key}.'.format(key=key))
    try:
        response = s3.get_object(
            Bucket=FILE_BUCKET,
            Key=key
        )
        bytes = response['Body'].read()
        return bytes.decode("UTF-8")
    except s3.exceptions.NoSuchKey:
        return ''


def save(key: str, contents: str):
    log.info('Saving file {key}.'.format(key=key))
    s3.put_object(
        ACL='private',
        Body=contents.encode("UTF-8"),
        Bucket=FILE_BUCKET,
        Key=key
    )
