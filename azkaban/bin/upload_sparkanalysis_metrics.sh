#!/usr/bin/env bash

BUCKET=$1
ENV=$2
JOB_CLASS=$3
RMHOST=$4
MAP_JAR=$5

CONF_FILE="/"${ENV}"_emr.conf"
YESTERDAY=$(date --date="1 days ago" +%Y-%m-%d)

SPARK_JARS_PATH="/opt/spark-2.3.1-bin-without-hadoop/jars/*"

for i in $(aws s3api list-objects-v2 --bucket ${BUCKET} --prefix ${ENV} --query "Contents[?contains(LastModified, '"${YESTERDAY}"')].Key" | tail -n +2 | head -n-1); do
    FILE=${i//\"}
    FILE_PATH="${BUCKET}/${FILE//,}"
    echo Running spark anlysis metrics job with file path: ${FILE_PATH}
    java -Dconfig.resource=${CONF_FILE} -cp ${SPARK_JARS_PATH}:${MAP_JAR} ${JOB_CLASS} ${FILE_PATH} ${RMHOST};
done;