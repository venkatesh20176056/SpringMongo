from paytm.async_result.async_result_dao import AsyncResultDao
from paytm.azkaban import AzkabanClient
from paytm.cma import CmaClient
from paytm.job.job_dao import JobDao
from paytm.job.job_queue import JobQueue
from paytm.job.stage_proc.rule_engine_stage import RuleEngineStageProc
from paytm.look_alike import env_vars as env
from paytm.look_alike.look_alike_stage import LookAlikeStageProc
from paytm.look_alike.update_segments_stage import UpdateSegmentsStageProc
from paytm.rule_engine import RuleEngineClient

cma_client = CmaClient(env.CMA_URL, env.TENANT)
rule_engine_client = RuleEngineClient(env.RULE_ENGINE_URL, env.TENANT, env.RULE_ENGINE_CLIENT_ID)
azkaban_client = AzkabanClient(env.AZKABAN_URL)
job_dao = JobDao(env.AWS_REGION, env.DYNAMO_TABLE_JOBS, env.DYNAMO_ENDPOINT_URL)
async_result_dao = AsyncResultDao(env.AWS_REGION, env.DYNAMO_TABLE_RESULT, env.DYNAMO_ENDPOINT_URL)
rule_engine_proc = RuleEngineStageProc(rule_engine_client)
look_alike_proc = LookAlikeStageProc(
    azkaban_client, env.AZKABAN_PROJECT, env.AZKABAN_LOOKALIKE_FLOW,
    env.LOOKBACK_DAYS, env.SEGMENT_SIZE_LIMIT,
    env.OUTPUT_BUCKET, env.LOOKALIKE_CONCAT_N,
    env.LOOKALIKE_DISTANCE_THRESHOLD, env.LOOKALIKE_KENDALL_THRESHOLD, env.TAG, env.ENV, env.CMA_URL, env.TENANT
)


job_queue = JobQueue(job_dao, rule_engine_proc, look_alike_proc)
