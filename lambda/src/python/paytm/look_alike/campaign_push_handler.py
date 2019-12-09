from datetime import datetime
import logging

from paytm.job import Job, JobType, Status
from paytm.job.stage_proc.rule_engine_stage import mk_rule_engine_stage
from paytm.look_alike.assembly import cma_client, job_dao
from paytm.look_alike.env_vars import TENANT, SEGMENT_SIZE_LIMIT
from paytm.look_alike.look_alike_stage import mk_look_alike_stage
from paytm.look_alike.update_segments_stage import mk_update_segments_stage
from paytm.tenant import TENANT_TIMEZONES
from paytm.time.time import as_utc, format_date, format_time
from paytm.look_alike import env_vars

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def boost_campaign(campaign_id: int) -> None:

    tenant_timestamp = datetime.now(TENANT_TIMEZONES[TENANT])
    tenant_date = tenant_timestamp.date()

    # by using cma_client, get camp info
    campaign = cma_client.get_scheduled_campaign(tenant_date, campaign_id)

    if campaign:
        job = _mk_push_job(tenant_timestamp, campaign)
        existing_job = job_dao.load(
            date_utc=format_date(as_utc(tenant_timestamp)),
            id=job.id
        )
        if existing_job is None or existing_job.status == Status.FAILED:
            job_dao.persist(job)
        else:
            _log.warning('Ignoring push triggered job {0} because it already exists with status {1}.'.format(
                existing_job.id, existing_job.status))


def _mk_push_job(tenant_timestamp: datetime, campaign: dict) -> Job:
    campaign_id: int = campaign['id']
    segment_id = 'campaign_{0}'.format(campaign_id)
    boost_limit = campaign.get('audienceBoost', SEGMENT_SIZE_LIMIT)
    campaign_details = cma_client.get_campaign_details(campaign_id)

    tags_arr = campaign_details['tags']
    tag_names = [i['name'].lower() for i in tags_arr]
    tag: str = ''.join(i for i in tag_names if 'boost' in i) if tag_names else 'default'

    _log.info("Found the tag-{tag} for campaign_id-{campaign_id}".format(tag=tag, campaign_id=campaign_id))

    rule_engine_stg = mk_rule_engine_stage(campaign_id, campaign['inclusion'], campaign['exclusion'])

    return Job(
        id=_job_id(campaign_id, tenant_timestamp),
        timestamp=tenant_timestamp,
        job_type=JobType.PUSH,
        metadata={
            'tenant_timestamp': format_time(tenant_timestamp)
        },
        stages=[
            rule_engine_stg,
            mk_look_alike_stage(segment_id, tenant_timestamp, boost_limit, tag)
        ]
    )


def _job_id(campaign_id: int, tenant_timestamp: datetime):
    return '{0}_push_campaign_{1}_{2}'.format(TENANT, campaign_id, format_date(tenant_timestamp))
