package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.Constants._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object MerchantAggGAFeatures extends MerchantAggGAFeaturesJob
  with SparkJob with SparkJobBootstrap

trait MerchantAggGAFeaturesJob {
  this: SparkJob =>

  val JobName = "MerchantAggGAFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    //GA specific dates
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val dtGASeq = dayIterator(targetDateGA.minusDays(gaBackFillDays), targetDateGA, ArgsUtils.formatter)

    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val aggGABasePath = s"${settings.featuresDfs.baseDFS.aggPath}/MerchantGAFeatures/"
    val merchantProfilePath = s"${settings.featuresDfs.baseDFS.aggPath}/MerchantProfile/dt=$targetDateStr"
    val merchantProfile = spark.read.parquet(merchantProfilePath)

    val aggregates = Seq(
      sum($"pdp_sessions_app").as("total_pdp_sessions_app"),
      sum($"pdp_sessions_web").as("total_pdp_sessions_web"),
      max(to_date($"dt")).alias("last_seen_all_date")
    )
    val gaAggregates = spark.read.parquet(gaAggPath)
      .join(merchantProfile, Seq("customer_id"), "right")
      .select(
        "merchant_id",
        "dt",
        "pdp_sessions_app",
        "pdp_sessions_web"
      )
      .groupPivotAgg(groupByCol = Seq("merchant_id", "dt"), "", aggregates)
      .renameColumns(prefix = "MGA_", excludedColumns = Seq("merchant_id", "dt"))
      .repartition(10)

    // moving ga features to respective directory
    gaAggregates.moveHDFSData(dtGASeq, aggGABasePath)
  }

}