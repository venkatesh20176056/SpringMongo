#!/bin/sh

#########################################
# Azkaban Flow Executer
#
#   This script will kick of run_pipeline.py n times for any project or flow in Azkaban midgar-stg or midgar-prod.
#
#########################################

project=${1}
flow=${2}
startdate=${3}
numberdays=${4}
environment=${5:-"staging"}
disableFile=${6:-"empty"}

usage () {
    echo "usage: $0 project(Midgar-Base, ...) flow(BaseTableFlow, ...) startdate('YYYY-MM-dd') nDays(ndays to run) environment('staging' or 'production') disabledFile"
    echo "disabled file contains one line for each item to be excluded from the flow"
    echo "example $0 Midgar-Base BaseTableFlow 2016-05-13 10 staging disabled.txt"
    echo "use "None" as the parameter for startdate if no start date needs to be specified"
}

disabled () {
    echo $1
    dflows="["
    for f in `cat $1`; do
        dflows+='"'$f'",'
    done
    dflows=${dflows%?}
    dflows+="]"
}

if [ "$#" -lt 5 ]; then
    echo "Illegal number of parameters - 5 are required"
    usage
    exit 1
fi

export AZKABAN_USERNAME=azkaban
# Modify the two email parameters below based on your own needs
#export AZKABAN_FAILURE_EMAIL=
#export AZKABAN_SUCCESS_EMAIL=
export AZKABAN_PROJECT_NAME=${project}
export AZKABAN_FLOW_NAME=${flow}
    
if [ ${environment} == "production" ] ; then
    echo "PRODUCTION RUN"
    export AZKABAN_HOST_NAME=http://10.141.17.27:8099/

elif [ ${environment} == "staging" ]; then
    echo "STAGING RUN"
    export AZKABAN_HOST_NAME=http://10.141.17.27:8105/

else
    echo "ERROR: Invalid environment specified"
    usage
    exit 1
fi

if [ ${disableFile} == "empty" ]; then
    dflows="[]"
elif [ -f ${disableFile} ]; then
    disabled ${disableFile}
else
    echo "Disable file non-existent"
    usage
    exit 1
fi

export AZKABAN_DISABLED_STEPS=$dflows

echo "Enter your password:"
read -s ENTERED_PASSWORD
export AZKABAN_PASSWORD=${ENTERED_PASSWORD}

for i in `seq 0 $numberdays`; do
    if [ ${startdate} == "None" ]; then
        export AZKABAN_TARGET_DATE=$startdate
    else
        rundate=`date -j -v +"$i"d -f "%Y-%m-%d" "$startdate" +%Y-%m-%d`
        export AZKABAN_TARGET_DATE=$rundate
    fi
    python run_pipeline.py
done
