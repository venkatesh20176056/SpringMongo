package com.paytm.map.featuresanalysis

import java.util.Properties

import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, FileUtils, Settings, UDFs}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, lit, when}

object FeatureUsageReport extends FeatureUsageReportJob
  with SparkJob with SparkJobBootstrap

trait FeatureUsageReportJob {
  this: SparkJob =>

  val JobName = "FeatureUsageReport"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    import settings.featuresDfs

    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val dbNames = args(1).split(",")
    val env = settings.environment

    val featureToCampaignIdQuery =
      """
        |(select feature, c.id as campaign_id from campaign c join (
        |select campaign_id, segment_id from (
        | select * from campaign_includes_segment union
        | select * from campaign_includes_segment_filter union
        | select * from campaign_excludes_segment union
        | select * from campaign_excludes_segment_filter
        | ) as ie ) as cis
        |on c.id = cis.campaign_id
        |join (
        |select id, type, feature from (
        |    select id, type, substring_index(substring_index(features, '|', 2), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 3), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 4), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 5), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 6), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 7), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 8), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 9), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 10), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 11), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 12), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 13), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 14), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 15), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 16), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 17), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 18), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 19), '|', -1) as feature from segment union
        |    select id, type, substring_index(substring_index(features, '|', 20), '|', -1) as feature from segment
        |) as f
        |where feature != ''
        |) s on s.id = cis.segment_id
        |WHERE c.state='STARTED'
        |and s.type = 'RULE_BASED'
        |and s.feature not like '%[]'
        |) a
        |""".stripMargin

    dbNames.foreach { dbName =>
      val featureToCampaignId = readCmaTable(settings, dbName, featureToCampaignIdQuery)(spark).as[FeatureCampaignId]

      val featureListPath = dbName match {
        case "paytm-india" => s"${featuresDfs.resources}${featuresDfs.featureList}"
        case "paytm-canada" =>
          val latestTablePath = FileUtils.getLatestTablePath(s"${featuresDfs.resources}/feature_list_canada", spark, targetDateStr, "updated_at=", featuresDfs.canadaFeatureList).get
          s"$latestTablePath/${featuresDfs.canadaFeatureList}"
        case "paytm-merchant-india" => s"${featuresDfs.resources}${featuresDfs.merchantFeatureList}"
      }
      val featureList = spark
        .read
        .option("header", "true")
        .option("numPartitions", 1)
        .csv(featureListPath)
        .withColumnRenamed("Field", "Name")
        .withColumnRenamed("Type", "DataType")
        .as[FeatureListItem]

      val allFeatureToCampaignId = featureToCampaignId
        .join(featureList, featureToCampaignId("feature") === featureList("Name"), "outer")
        .withColumn("feature", when('campaign_id.isNull, lit("unused")).otherwise('feature))
        .as[FeatureUsageJoined]
        .coalesce(1)
        .cache()

      val allfeatureUsage = allFeatureToCampaignId
        .groupBy('feature)
        .agg(count(lit(1)) as "campaignCount")
        .as[FeatureUsage]

      allFeatureToCampaignId
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${settings.featuresDfs.featureUsageReport}/tenant=$dbName/dt=$targetDateStr")

      allfeatureUsage
        .collect()
        .foreach { usage =>
          UDFs.gaugeValuesWithTags(
            "feature_usage_campaign_count",
            Map("env" -> env, "tenant" -> dbName, "feature" -> usage.feature),
            usage.campaignCount
          )
        }
    }
  }

  def readCmaTable(settings: Settings, dbName: String, tableQuery: String)(implicit spark: SparkSession): DataFrame = {
    val cmaUrl = getJDBCUrl(settings.cmaRds.host, settings.cmaRds.port, dbName)
    val connectionProperties = getJDBCConnectionProperties(
      "com.mysql.jdbc.Driver",
      settings.cmaRds.user, settings.getParameter(
        settings.cmaRds.passwordPath,
        s"/map-measurements/prod"
      )
    )

    spark.read.option("numPartitions", 1).jdbc(url = cmaUrl, table = tableQuery, properties = connectionProperties)
  }

  def getJDBCUrl(host: String, port: Int, db: String): String = s"jdbc:mysql://$host:$port/$db"

  def getJDBCConnectionProperties(driver: String, user: String, pwd: String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", driver)
    connectionProperties.put("user", s"$user")
    connectionProperties.put("password", s"$pwd")
    connectionProperties
  }
}

case class FeatureUsage(feature: String, campaignCount: Long)
case class FeatureCampaignId(feature: String, campaign_id: Long)
case class FeatureListItem(Class: String, Vertical: String, Category: String, Name: String,
  Alias: String, Datatype: String, Description: String) // isEnable: Int, ExecLevel: String, isExposed: Int)
case class FeatureUsageJoined(feature: String, campaign_id: Long,
  Class: String, Vertical: String, Category: String, Name: String,
  Alias: String, Datatype: String, Description: String)
