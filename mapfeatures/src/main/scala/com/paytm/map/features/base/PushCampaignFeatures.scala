package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.BaseDFSConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, max, to_date}

object PushCampaignFeatures extends PushCampaignFeaturesJob
  with SparkJob with SparkJobBootstrap

trait PushCampaignFeaturesJob {
  this: SparkJob =>

  val JobName = "CampaignInteractionFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args).minusDays(1)
    val targetDateString = targetDate.toString(ArgsUtils.formatter)
    val lookBackDays: Int = args(1).toInt
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    val baseDFS = settings.featuresDfs.baseDFS
    val aggUaPath = s"${baseDFS.aggPath}/PushFeatures"

    val urbanAirshipLogPath = settings.featuresDfs.urbanAirship

    val uaBase = DataTables.urbanAirshipData(spark, urbanAirshipLogPath, targetDateString, startDateStr)
    val pushLogs = DataTables.pushLogs(spark, settings.datalakeDfs.pushLogPath, targetDateString, startDateStr)

    val pushData = DataTables.pushCampaignTable(uaBase, pushLogs)

    val lastReceived = pushData
      .filter($"push_type" === "RECEIVED")
      .groupBy("dt", "customer_id")
      .agg(
        max($"occurred_date").alias("last_push_receive_date")
      )

    val uaAggregate = pushData
      .filter($"push_type" === "INTERACTION")
      .groupBy("dt", "customer_id")
      .agg(
        collect_set($"campaign_id").alias("push_campaigns_interacted_with"),
        max($"occurred_date").alias("last_push_interaction_date")
      )

    val pushFeatures = lastReceived
      .join(uaAggregate, Seq("customer_id", "dt"), "full_outer")
      .coalescePartitions("dt", "customer_id", dtSeq)
      .cache()

    pushFeatures.moveHDFSData(dtSeq, aggUaPath)
  }
}