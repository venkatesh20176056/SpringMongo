package com.paytm.map.features.utils

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, GetCallerIdentityRequest}
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest
import com.paytm.map.features.config._
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable

/**
 * Application settings. First attempts to acquire from the deploy environment.
 * If not exists, then from -D java system properties, else a default config.
 *
 * Settings in the environment such as: SPARK_HA_MASTER=local[10] is picked up first.
 *
 * Settings from the command line in -D will override settings in the deploy environment.
 * For example: sbt -Dspark.master="local[12]" run
 *
 * Any of these can also be overriden by your own application.conf.
 *
 * @param conf Optional config for test
 */

final class Settings(conf: Option[Config] = None) {

  protected def getConfigFromParameterStore(awsConf: AwsConfig, pathPrefix: String = "/common/"): Map[String, String] = {
    val assumeRoleRequest = new AssumeRoleRequest
    val getCallerIdentityRequest = new GetCallerIdentityRequest

    val stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient()
    val currentRole = stsClient.getCallerIdentity(getCallerIdentityRequest)

    val stsResponse = stsClient.assumeRole(
      assumeRoleRequest
        .withRoleArn(awsConf.parameterStore.roleArn)
        .withRoleSessionName(currentRole.getArn.split("/")(1))
    )

    val sessionCredentials = stsResponse.getCredentials
    val basicSessionCredentials = new BasicSessionCredentials(
      sessionCredentials.getAccessKeyId,
      sessionCredentials.getSecretAccessKey,
      sessionCredentials.getSessionToken
    )

    val ssmClient = AWSSimpleSystemsManagementClientBuilder.standard()
      .withRegion("ap-south-1")
      .withCredentials(new AWSStaticCredentialsProvider(basicSessionCredentials)).build()

    val parameterMap: mutable.Map[String, String] = mutable.Map.empty

    {
      val getParametersRequest = new GetParametersByPathRequest()
        .withPath(pathPrefix)
        .withRecursive(true)
        .withWithDecryption(true)

      val ssmResponse = ssmClient.getParametersByPath(getParametersRequest)

      val parametersIterator = ssmResponse.getParameters.iterator()
      while (parametersIterator.hasNext) {
        val parameter = parametersIterator.next()
        parameterMap += (parameter.getName -> parameter.getValue)
      }
    }
    parameterMap.toMap
  }

  val rootConfig: Config = conf match {
    case Some(c) => c.withFallback(ConfigFactory.load())
    case _       => ConfigFactory.load()
  }

  protected val featuresDfsConfig: Config = rootConfig.getConfig("dfs")
  protected val campaignDfsConfig: Config = rootConfig.getConfig("dfs_campaigns")
  protected val datalakeDfsConfig: Config = rootConfig.getConfig("dfs_datalake")
  protected val midgarDfsConfig: Config = rootConfig.getConfig("dfs_midgar_workspace")
  protected val jobConfig = rootConfig.getConfig("job")
  protected val esExportConfig = rootConfig.getConfig("es_export")
  protected val awsConfig = rootConfig.getConfig("aws")
  protected val redshiftConfig = rootConfig.getConfig("redshift")
  protected val contextDataConfig = rootConfig.getConfig("context_data")
  protected val eSHeroConfig = rootConfig.getConfig("es_hero")
  protected val shinraDfsConfig = rootConfig.getConfig("shinra")

  protected val bankDfsConfig = rootConfig.getConfig("dfs_bank")
  protected val cmaRdsConfig = rootConfig.getConfig("cma_rds")

  val environment = rootConfig.getString("env")
  val midgarS3Bucket = rootConfig.getString("bucket_name")
  val cmaApi = rootConfig.getString("cma_api")
  val featuresDfs = DfsConfig(featuresDfsConfig)
  val shinraDfs = ShinraDfsConfig(shinraDfsConfig)
  val campaignsDfs = CampaignDfsConfig(campaignDfsConfig)
  val awsCfg = AwsConfig(awsConfig)
  val bankDfs = BankDfsConfig(bankDfsConfig)
  val cmaRds = CmaRdsConfig(cmaRdsConfig)

  val midgarDfs = MidgarDfsConfig(midgarDfsConfig)
  val jobCfg = JobConfig(jobConfig)
  val esExportCfg = EsExportConfig(esExportConfig)
  lazy val datalakeDfs = DatalakeDfsConfig(datalakeDfsConfig)
  lazy val redshiftDb = RedshiftConfig(redshiftConfig)
  lazy val contextData = ContextDataConfig(contextDataConfig)
  lazy val heroFeaturesES = ESConfig(eSHeroConfig)
  lazy val userTokenCassandraCfg = CassandraConfig(rootConfig.getConfig("userTokenCassandra"))
  lazy val deviceInfoCassandraCfg = CassandraConfig(rootConfig.getConfig("deviceInfoCassandra"))

  def getParameter(paramName: String) = {

    val awsParameterMap = getConfigFromParameterStore(awsCfg)
    awsParameterMap.get(paramName) match {
      case Some(value) => value
      case _           => throw new IllegalArgumentException(s"$paramName not found under AWS Parameter Store")
    }
  }

  def getParameter(paramName: String, pathPrefixEnv: String) = {

    val paramKey = s"$pathPrefixEnv/$paramName"
    val awsParameterMap = getConfigFromParameterStore(awsCfg, s"$pathPrefixEnv/")

    awsParameterMap.get(paramKey) match {
      case Some(value) => value
      case _           => throw new IllegalArgumentException(s"$paramKey not found under AWS Parameter Store")
    }
  }
}

