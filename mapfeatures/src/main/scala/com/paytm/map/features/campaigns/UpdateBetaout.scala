package com.paytm.map.features.campaigns

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import com.paytm.map.features.campaigns.CampaignConstants._
import org.apache.spark.sql.AnalysisException

object UpdateBetaout extends UpdateBetaoutJob with SparkJob with SparkJobBootstrap

trait UpdateBetaoutJob {
  this: SparkJob =>

  val JobName = "UpdateBetaout"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def readBetaoutData(spark: SparkSession, path: String, day: String): DataFrame = {
    spark.read.option("header", "true").csv(s"$path/$day.csv")
  }

  def transformBetaout(spark: SparkSession, betaoutDf: DataFrame, betaCond: Seq[(String, Column, Column)]): DataFrame = {
    import spark.implicits._

    val betaoutCols = betaoutDf
      .select(
        $"campaign_id" as "emailCampaignId",
        $"email_id",
        $"sent_time" as "sentTime",
        $"opened_time" as "openedTime"
      )
      .withColumn("sentTime", from_unixtime($"sentTime" / 1000).cast("timestamp"))
      .withColumn("openedTime", from_unixtime($"openedTime" / 1000).cast("timestamp"))

    betaoutCols
      .caseWhen(betaCond)
      .groupByAggregate(betaoutGroupBy, campaignBaseAggs)
      .drop("emailCampaignId")
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings.{featuresDfs, campaignsDfs}
    import spark.implicits._

    try {
      val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)

      val betaPath = campaignsDfs.betaoutEvents
      val outPath = s"${featuresDfs.campaignsTable}/country=india/source=betaout/dt=$targetDateStr"

      val betaoutYday = readBetaoutData(spark, betaPath, targetDateStr)
      val betaoutTransformed = transformBetaout(spark, betaoutYday, betaConditions).cache()

      configureSparkForMumbaiS3Access(spark)

      betaoutTransformed.saveParquet(outPath, 20)
    } catch {
      case noCsv: org.apache.spark.sql.AnalysisException => log.warn("Yesterday's csv path does not exist")
    }

  }
}
