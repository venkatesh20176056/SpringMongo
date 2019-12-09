package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.Constants.{gaBackFillDays, gaDelayOffset}
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object LastSeenAgg extends LastSeenAggJob
  with SparkJob with SparkJobBootstrap

trait LastSeenAggJob {
  this: SparkJob =>

  val JobName = "LastSeenAgg"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    import spark.implicits._

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val gaBasePath = s"${settings.featuresDfs.baseDFS.aggPath}/LastSeenAgg"

    //GA specific dates
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val startDateGA = targetDateGA.minusDays(gaBackFillDays - 1) // -1 so exactly gaBackFillDays is run
    val dtGASeq = dayIterator(startDateGA, targetDateGA, ArgsUtils.formatter)

    val gaData = DataTables.gaJoinedWebAppRaw(
      spark,
      settings.datalakeDfs.gaAppPaths,
      settings.datalakeDfs.gaWebPaths,
      settings.datalakeDfs.gaMallAppPaths,
      settings.datalakeDfs.gaMallWebPaths,
      targetDateGA,
      startDateGA
    ).groupBy("customer_id", "dt")
      .agg(
        countDistinct(when($"isApp" === 0 && $"isMall" === 0, $"session_identifier").otherwise(null)).as("paytm_web"),
        countDistinct(when($"isApp" === 0 && $"isMall" === 1, $"session_identifier").otherwise(null)).as("mall_web"),
        countDistinct(when($"isApp" === 1 && $"isMall" === 0, $"session_identifier").otherwise(null)).as("paytm_app"),
        countDistinct(when($"isApp" === 1 && $"isMall" === 1, $"session_identifier").otherwise(null)).as("mall_app")
      )
      .groupBy("customer_id", "dt")
      .agg(getLastSeenAggs.head, getLastSeenAggs.tail: _*)
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    gaData.moveHDFSData(dtGASeq, gaBasePath)

  }

  val channels: Seq[String] =
    Seq(
      "paytm_web",
      "paytm_app",
      "mall_web",
      "mall_app"
    )

  def getLastSeenAggs: Seq[Column] = {
    channels.map(channel => max(when(col(channel).gt(0), to_date(col("dt"))).otherwise(null)).alias(channel)) ++
      Seq(max(to_date(col("dt"))).alias("all"))
  }

}
