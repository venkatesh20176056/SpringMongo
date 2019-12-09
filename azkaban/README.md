# Glue Crawler Job

## Setup Pants
Run `./pants` from the repo root directory. The first time pants-build runs, it will bootstrap
itself, which takes a couple of minutes. Pants-build does not change your Python installations
in any way.

## Test and Type-check
./pants test mypy azkaban/:: -- --ignore-missing-imports

## Python Repl
./pants repl azkaban/bin:glue-crawler-job

## Run
./pants run azkaban/bin:glue-crawler-job -- $ENV $AWS_ROLE $DB_NAME $GROUP_ID $DATE
./pants run azkaban/bin:glue-crawler-job -- dev test_role test_db 1 2018-08-10

## Package
./pants bundle azkaban/bin:glue-crawler-job

## Run Artifact
./dist/glue-crawler-job.pex $ENV $AWS_ROLE $DB_NAME $GROUP_ID $DATE
./dist/glue-crawler-job.pex dev test_role test_db 1 2018-08-10

## Test, Package, and Deploy
Azkaban server has python 3.5, so you must uncomment `compatibility='CPython>=3.5,<3.6'` override
in `azkaban/bin/BUILD` to produce a pex that runs on azkaban. This is disabled by default because having
multiple Python versions confuses IntelliJ.
./pants test mypy azkaban/:: -- --ignore-missing-imports && \
  ./pants bundle azkaban/bin:glue-crawler-job && \
  scp -i $MIDGAR_PEM ./dist/glue-crawler-job.pex ubuntu@$AZKABAN_HOST:glue-crawler-job.pex

## Staging Test
./glue-crawler-job.pex stg service-role/AWSGlueServiceRole-alpha daily_flatfeatures 2 2018-08-10

## Prod Backfill
DATE=2018-08-16; \
  ./glue-crawler-job.pex prod service-role/AWSGlueServiceRole-alpha daily_flatfeatures 1 $DATE && \
  ./glue-crawler-job.pex prod service-role/AWSGlueServiceRole-alpha daily_flatfeatures 2 $DATE && \
  ./glue-crawler-job.pex prod service-role/AWSGlueServiceRole-alpha daily_flatfeatures 3 $DATE && \
  ./glue-crawler-job.pex prod service-role/AWSGlueServiceRole-alpha daily_flatfeatures 4 $DATE
