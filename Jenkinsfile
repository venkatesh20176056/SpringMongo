#!/usr/bin/env groovy
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException
import java.text.SimpleDateFormat


def inputNotificationSent = false
def deployBranches = ["master","develop"]
def bitbucket = new com.paytmlabs.jenkins.test.BitBucket(this)


def deployUser = "dl-deploy"
def deployDirectory = "/opt/map-features"
def hosts = ""
def targetPackage = ""
def zeppelinHosts = ""


if (env.BRANCH_NAME == 'master' || env.BRANCH_NAME.startsWith('hotfix')) {
    env.TARGET_ENV = 'prod'
    targetPackage = "packageFeaturesProd"
    hosts = "172.21.11.111"
} else {
    env.TARGET_ENV = 'staging'
    targetPackage = "packageFeaturesStaging"
    hosts = "172.21.9.144"
    zeppelinHosts = "172.21.155.144"
}

properties([
        parameters([
                getAutoDeploy(),
        ]),
        buildDiscarder(logRotator(artifactNumToKeepStr: '5', numToKeepStr: '5')),
        pipelineTriggers([pollSCM('*/5 * * * *')])
])

try {
    node {

        deleteDir()
        checkout scm

        def runTest = bitbucket.shouldTest(currentBuild)
        echo "runTest: $runTest"

        stage('Build') {
            milestone 1
            sbt "clean update compile"
        }

        if (runTest) {
            stage('Test') {
                milestone 2
                sbt "test"
            }
        } else {
            echo "Skipping test, the commit was successfully tested before"
        }

        stage('Package') {
            milestone 3
            sbt "'project mapfeatures' assembly"
            sbt "'project mapfeatures' '$targetPackage'"

            archiveArtifacts artifacts: "**/target/**/*.zip", fingerprint: true
            archiveArtifacts artifacts: "**/target/**/*.jar", fingerprint: true
        }


    }

    if (params.autoDeploy || env.BRANCH_NAME in deployBranches) {
        stage('Deploy') {
            if (currentBuild.result == null || currentBuild.result == 'SUCCESS') {
                milestone 4
                def shouldDeploy = true
                if (env.BRANCH_NAME == "master") {
                    def msg = "Do you want to deploy?"
                    //TODO: No notify method. Please find a substitute for this
                    //notify("Waiting you input. ${msg}", "warning")
                    //inputNotificationSent = true
                    try {
                        timeout(time: 4, unit: 'HOURS') {
                            input id: 'Deploy', message: msg
                            echo "Deploying ${params.services} ..."
                        }
                    } catch (FlowInterruptedException ex) {
                        // Not needed, but left for reference to be used with rollback flow
                        echo "Flow interrupted ${ex}"
                        currentBuild.result = "ABORTED"
                        shouldDeploy = false
                    }
                }
                if (shouldDeploy) {
                    milestone 5
                    node {
                        def hostValues = hosts.split(' ')
                        for (int i = 0; i < hostValues.size(); i++) {
                            host = hostValues[i]
                            sh "deploy/deploy_azkaban_bin.sh $env.TARGET_ENV $deployUser $host $deployDirectory"
                            withCredentials([string(credentialsId: 'azkaban_pass', variable: 'PW1')])
                            {
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Raw rawflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Snapshot snapshotflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Export exportflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-LookAlike lookalikeflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Campaigns campaignflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Redshift redshiftflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Schema-Migration schemamigration $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Models modelflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Merchant merchantflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Channel channelflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-Metrics analysisflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                                sh "deploy/deploy_azkaban_flow.sh Map-Features-DevicePush devicepushflow $env.TARGET_ENV $deployUser $host $deployDirectory ${PW1}"
                            }
                        }
                        def zeppelinHostsValues = zeppelinHosts.split(',')
                        for (int i = 0; i < zeppelinHostsValues.size(); i++) {
                            host = zeppelinHostsValues[i]
                            if (host != "") {
                                sh "deploy/deploy_azkaban_bin.sh $env.TARGET_ENV $deployUser $host $deployDirectory"
                            }
                        }
                    }
                } else {
                    echo "Skip deployment"
                }
            }
        }
    }

    node {
        stage('Copy resources to S3 bucket') {
            def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
            def date = new Date()
            def currentDate = dateFormat.format(date)
            def envPath = 'stg'

            if (env.TARGET_ENV == 'prod') {
                envPath = 'prod'
            }

            sh """
               aws s3 cp azkaban/resources/feature_list_canada.csv s3://midgar-aws-workspace/$envPath/mapfeatures/resources/feature_list_canada/updated_at=$currentDate/feature_list_canada.csv
               aws s3 cp azkaban/resources/category_map.csv s3://midgar-aws-workspace/$envPath/mapfeatures/resources/uploacategory_mapping/
               aws s3 cp azkaban/resources/external_client_id_mapping.csv s3://midgar-aws-workspace/$envPath/mapfeatures/resources/external_client_id_mapping/
               aws s3 cp azkaban/resources/no_export s3://midgar-aws-workspace/$envPath/mapfeatures/no_export
            """

        }
    }

} finally {
    if (inputNotificationSent) {
        if (currentBuild.result == null || currentBuild.result == "SUCCESS") {
            notify("Pipeline finished successfully", "good")
        } else if (currentBuild.result == "FAILURE") {
            notify("Pipeline failed", "danger")
        } else if (currentBuild.result == "ABORTED") {
            notify("Pipeline aborted", "warning")
        } else {
            notify("Pipeline status: ${currentBuild.result}", "warning")
        }
    }
}



def getAutoDeploy() {
    if (env.BRANCH_NAME == 'develop') {
        booleanParam(defaultValue: true, description: 'Auto deploy when build is successful?', name: 'autoDeploy')
    } else {
        booleanParam(defaultValue: false, description: 'Auto deploy when build is successful?', name: 'autoDeploy')
    }
}

