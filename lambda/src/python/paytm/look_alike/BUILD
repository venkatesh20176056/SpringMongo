python_binary(
  name = 'look-alike-lambda',
  dependencies = [
    ':look_alike'
  ],
  source = 'look_alike_lambda.py'
)


python_library(
  dependencies = [
    'lambda/src/python/paytm/azkaban',
    'lambda/src/python/paytm/async_result',
    'lambda/src/python/paytm/ssm',
    'lambda/src/python/paytm/cma',
    'lambda/src/python/paytm/job',
    'lambda/src/python/paytm/job/stage_proc',
    'lambda/src/python/paytm/tenant',
    'lambda/src/python/paytm/rule_engine',
  ],
  sources = globs('*.py', exclude=['look_alike_lambda.py'])
)
