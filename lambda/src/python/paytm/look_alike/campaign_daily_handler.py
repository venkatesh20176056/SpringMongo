from datetime import datetime, date

from paytm.job import Job, JobType
from paytm.job.stage_proc.rule_engine_stage import mk_rule_engine_stage
from paytm.look_alike.assembly import cma_client, job_dao
from paytm.look_alike.env_vars import TENANT, SEGMENT_SIZE_LIMIT
from paytm.look_alike.look_alike_stage import mk_look_alike_stage, remove_look_alike_segments_old_cma, \
    remove_look_alike_segments
from paytm.look_alike.update_segments_stage import mk_update_segments_stage
from paytm.tenant import TENANT_TIMEZONES
from paytm.time.time import format_date, format_time
from paytm.look_alike import env_vars


def start_daily_dataset_update():
    tenant_timestamp = datetime.now(TENANT_TIMEZONES[TENANT])
    campaigns = cma_client.get_campaigns_with_boost()
    for campaign in campaigns:
        job = _mk_daily_job(tenant_timestamp, campaign)
        job_dao.persist(job)


def _mk_daily_job(tenant_timestamp: datetime, scheduled_campaign: dict) -> Job:
    campaign_id: int = scheduled_campaign['id']
    campaign_details = cma_client.get_campaign_details(campaign_id)

    tags_arr = campaign_details['tags']
    tag_names = [i['name'].lower() for i in tags_arr]
    tag: str = ''.join(i for i in tag_names if 'boost' in i) if tag_names else 'default'

    temp_campaign = remove_look_alike_segments_old_cma(scheduled_campaign, campaign_details)
    campaign = remove_look_alike_segments(temp_campaign)
    segment_id = 'campaign_{0}'.format(campaign_id)
    boost_limit = campaign.get('audienceBoost', SEGMENT_SIZE_LIMIT)
    inclusion = campaign.get('inclusion', {'segments': campaign['includedSegments']})
    exclusion = campaign.get('exclusion', {'segments': campaign['excludedSegments']})

    rule_engine_stg = mk_rule_engine_stage(campaign_id, inclusion, exclusion)

    return Job(
        id=_job_id(campaign_id, tenant_timestamp),
        timestamp=tenant_timestamp,
        job_type=JobType.DAILY,
        metadata={
            'tenant_timestamp': format_time(tenant_timestamp)
        },
        stages=[
            rule_engine_stg,
            mk_look_alike_stage(segment_id, tenant_timestamp, boost_limit, tag)
        ]
    )


def _job_id(campaign_id: int, tenant_timestamp: datetime):
    return '{0}_daily_{1}_{2}'.format(TENANT, campaign_id, format_date(tenant_timestamp))
