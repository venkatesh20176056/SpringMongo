import logging
import os

from paytm.tenant import TENANTS


ENV = os.environ['MAP_ENV']  # dev | stg | prod
if ENV == 'dev':
    logging.basicConfig(level=logging.INFO)

TENANT = os.environ['MAP_TENANT']  # 'paytm-canada' | 'paytm-india'
assert TENANT in TENANTS
LOOKBACK_DAYS = int(os.environ['MAP_LOOKBACK_DAYS'])  # Sales look back days for model
SEGMENT_SIZE_LIMIT = int(os.environ['MAP_SEGMENT_SIZE_LIMIT'])

# currently configured as lambda env. Later to be changed to payload values
TAG = 'default'
LOOKALIKE_DISTANCE_THRESHOLD = os.environ['MAP_LOOKALIKE_DISTANCE_THRESHOLD']
LOOKALIKE_KENDALL_THRESHOLD = os.environ['MAP_LOOKALIKE_KENDALL_THRESHOLD']
LOOKALIKE_CONCAT_N = os.environ['MAP_LOOKALIKE_CONCAT_N']

CMA_URL = os.environ['MAP_CMA_URL']
RULE_ENGINE_URL = os.environ['MAP_RULE_ENGINE_URL']
RULE_ENGINE_CLIENT_ID = os.environ['MAP_RULE_ENGINE_CLIENT_ID']  # 'MAP_BANNER_PIPELINE'

AZKABAN_URL = os.environ['MAP_AZKABAN_URL']  # protocol://user:password@host:port
AZKABAN_PROJECT = os.environ['MAP_AZKABAN_PROJECT']
AZKABAN_LOOKALIKE_FLOW = os.environ['MAP_AZKABAN_LOOKALIKE_FLOW']
AZKABAN_UPLOAD_FLOW = os.environ['MAP_AZKABAN_UPLOAD_FLOW']


OUTPUT_BUCKET = os.environ['MAP_OUTPUT_BUCKET']

AWS_REGION = os.environ['MAP_AWS_REGION']  # ap-south-1 for service hosted in Mumbai
DYNAMO_TABLE_JOBS = os.environ['MAP_DYNAMO_TABLE_JOBS']
DYNAMO_TABLE_RESULT = os.environ['MAP_DYNAMO_TABLE_RESULT']
DYNAMO_ENDPOINT_URL = None

BETA_LOOKALIKE_CAMPAIGNS = os.environ['MAP_BETA_LOOKALIKE_CAMPAIGNS'].split(',')
