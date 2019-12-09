from datetime import datetime

import pytz

ISO_8601_UTC = '%Y-%m-%dT%H:%M:%SZ'
ISO_8601_OFFSET = '%Y-%m-%dT%H:%M:%S%z'
DATE_FORMAT = '%Y-%m-%d'


def format_time_utc(timestamp: datetime) -> str:
    ts_utc = as_utc(timestamp).replace(microsecond=0)
    return ts_utc.strftime(ISO_8601_UTC)


def parse_time_utc(timestamp: str) -> datetime:
    return pytz.utc.localize(datetime.strptime(timestamp, ISO_8601_UTC))


def format_time(timestamp: datetime) -> str:
    require_tzinfo(timestamp)
    return timestamp.replace(microsecond=0).strftime(ISO_8601_OFFSET)


def format_date(timestamp: datetime) -> str:
    require_tzinfo(timestamp)
    return timestamp.strftime(DATE_FORMAT)


def require_tzinfo(timestamp: datetime) -> None:
    if not timestamp.tzinfo:
        raise ValueError('`timestamp` must be timezone aware.')


def as_utc(timestamp: datetime) -> datetime:
    require_tzinfo(timestamp)
    return timestamp.astimezone(pytz.utc)
