#!/bin/bash +x
set -e

AZKABAN_PROJECT_NAME=$1
AZKABAN_FLOW_NAME=$2
DEPLOYMENT_ENV=$3
DEPLOY_USER=$4
REMOTE_HOST=$5
DEPLOY_DIR=$6

AZKABAN_USERNAME=azkaban
AZKABAN_UI_URL="http://"$REMOTE_HOST":8099/manager"
AZKABAN_PASSWORD=$7

WORKSPACE_DIR_NAME=$(pwd)
cd $WORKSPACE_DIR_NAME/mapfeatures/target/universal


RESOLVED_FILE_NAME=$(ls "mapfeatures_"$AZKABAN_FLOW_NAME"_"*)
RESOLVED_PROJECT_NAME=`echo $AZKABAN_PROJECT_NAME`
RESOLVED_BIN_FILE=$(ls mapfeatures_bin_*)
EXTRACTED_FILENAME=${RESOLVED_BIN_FILE%.*}

echo "Uploading $RESOLVED_FILE_NAME to azkaban $DEPLOYMENT_ENV environment"
scp  -o StrictHostKeyChecking=no $RESOLVED_FILE_NAME $DEPLOY_USER@$REMOTE_HOST:

ssh -tt $DEPLOY_USER@$REMOTE_HOST << EOF
  echo "Submitting $RESOLVED_FILE_NAME to the $RESOLVED_PROJECT_NAME"
  curl -f -i -H 'Content-Type: multipart/mixed' -X POST --form "username=$AZKABAN_USERNAME" --form "password=$AZKABAN_PASSWORD" --form "ajax=upload" --form file=@$RESOLVED_FILE_NAME\;type=application/zip --form "project=$RESOLVED_PROJECT_NAME" $AZKABAN_UI_URL

  exit
EOF

