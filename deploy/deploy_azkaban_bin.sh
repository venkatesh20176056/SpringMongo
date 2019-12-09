#!/bin/bash +x
set -e

DEPLOYMENT_ENV=$1
DEPLOY_USER=$2
REMOTE_HOST=$3
DEPLOY_DIR=$4

WORKSPACE_DIR_NAME=$(pwd)
cd $WORKSPACE_DIR_NAME/mapfeatures/target/universal


RESOLVED_BIN_FILE=$(ls mapfeatures_bin_*)
EXTRACTED_FILENAME=${RESOLVED_BIN_FILE%.*}

echo "Copying $RESOLVED_BIN_FILE to azkaban $DEPLOYMENT_ENV environment"
scp  -o StrictHostKeyChecking=no $RESOLVED_BIN_FILE  $DEPLOY_USER@$REMOTE_HOST:

ssh -tt $DEPLOY_USER@$REMOTE_HOST << EOF

  echo "Unzipping $RESOLVED_BIN_FILE to azkaban $DEPLOYMENT_ENV environment"
  unzip -o $RESOLVED_BIN_FILE -d $DEPLOY_DIR
  
  echo "Removing existing symlink"
  unlink $DEPLOY_DIR/mapfeatures.jar
  unlink $DEPLOY_DIR/bin
  unlink $DEPLOY_DIR/resources

  echo "Creating new symlink"
  ln -s `echo $DEPLOY_DIR/$EXTRACTED_FILENAME/mapfeatures_*.jar` $DEPLOY_DIR/mapfeatures.jar
  ln -s $DEPLOY_DIR/$EXTRACTED_FILENAME/bin $DEPLOY_DIR/bin
  ln -s $DEPLOY_DIR/$EXTRACTED_FILENAME/resources $DEPLOY_DIR/resources

  exit
EOF

