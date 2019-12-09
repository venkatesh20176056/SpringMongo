python_binary(
  name='glue-crawler-job',
  dependencies = [
    ':glue-crawler-lib'
  ],
  source='glue_crawler_job.py',
  #Note: Disabled as IntelliJ workaround
  #compatibility='CPython>=3.5,<3.6' # The Python version on Azkaban is 3.5 (Ubuntu 16.04).
)

python_library(
  name='glue-crawler-lib',
  dependencies = [
    '3rdparty/python:boto3',
    ':glue-crawler-resources',
  ],
  sources=['crawler.py', 'table_alias_file.py']
)

resources(
  name='glue-crawler-resources',
  sources=['group_exporter.json']
)
