package com.paytm.map.features.datasets

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateFSDataset extends CreateFSDatasetJob with SparkJob with SparkJobBootstrap

trait CreateFSDatasetJob {
  this: SparkJob =>

  val JobName = "CreateFSDataset"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val levelPrefix = args(1) // gaFS
    val groupPrefix = args(2)
    val customerSegmentPath = (featuresDfs.featuresTable + groupPrefix + s"/ids/dt=$targetDateStr").replace("/stg/", "/prod/")
    val clicksPath = (settings.shinraDfs.basePath + s"/banner_click/").replace("/stg/", "/prod/")
    val paytmmoneyCMAID = "9"

    // Output
    val outPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}/$levelPrefix"

    val last30Days = targetDate.minusDays(30).toString(ArgsUtils.formatter)
    val last60Days = targetDate.minusDays(60).toString(ArgsUtils.formatter)

    // Read datasets
    val customerSegment = spark.read.parquet(customerSegmentPath)
    val rawData = spark.read.parquet(clicksPath).
      filter($"engage_category" === paytmmoneyCMAID). // Filter for paytm money
      na.fill(0).
      filter($"banner_clicks" + $"icon_clicks" gt 0). // reduce data for group by
      join(customerSegment, Seq("customer_id"), "left_semi")

    import com.paytm.map.features.utils.ConvenientFrame._
    val features = rawData.
      select($"customer_id", $"banner_clicks", $"icon_clicks", $"dt").
      withColumn("last_30", $"dt" > lit(last30Days)).
      withColumn("last_60", $"dt" > lit(last60Days)).
      groupBy("customer_id").
      agg(
        sum("banner_clicks") as "banner_clicks",
        sum("icon_clicks") as "icon_clicks",
        sum(when($"last_30" === true, $"banner_clicks").otherwise(0)) as "banner_clicks_30_days",
        sum(when($"last_30" === true, $"icon_clicks").otherwise(0)) as "icon_clicks_30_days",
        sum(when($"last_60" === true, $"banner_clicks").otherwise(0)) as "banner_clicks_60_days",
        sum(when($"last_60" === true, $"icon_clicks").otherwise(0)) as "icon_clicks_60_days"
      ).
        withColumn("total_clicks_30_days", $"banner_clicks_30_days" + $"icon_clicks_30_days").
        withColumn("total_clicks_60_days", $"banner_clicks_60_days" + $"icon_clicks_60_days").
        withColumn("total_clicks", $"banner_clicks" + $"icon_clicks").
        addPrefixToColumns("paytmmoney_", Seq("customer_id"))

    features.write.mode(SaveMode.Overwrite).parquet(s"$outPath/dt=$targetDateStr")

  }
}