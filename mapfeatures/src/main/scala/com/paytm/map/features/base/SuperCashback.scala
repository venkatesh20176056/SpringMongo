package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CreateSuperCashbackDataset extends CreateSuperCashbackDatasetJob with SparkJob with SparkJobBootstrap

trait CreateSuperCashbackDatasetJob {
  this: SparkJob =>
  val JobName = "CreateSuperCashbackDataset"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings._
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val startDate = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    val baseDFSPath = featuresDfs.baseDFS
    val anchorPath = s"${baseDFSPath.anchorPath}/SuperCashback"
    val outPath = s"${baseDFSPath.aggPath}/superCashback/"

    val campaigns = DataTables.getCampaigns(spark, settings.datalakeDfs.campaigns)
    val promos = DataTables.getPromos(spark, settings.datalakeDfs.superCashbacks, targetDateStr, startDate)

    val filteredCampaigns = campaigns
      .withColumnRenamed("campaign", "campaign_name")
      .withColumnRenamed("id", "campaign_id")
      .drop("updated_at")
      .where('campaign_type === 3)
      .drop("created_at")

    //pass it from property file
    val filteredPromos = promos

    val finalSupercashbackData = {
      filteredPromos
        .skewJoin(filteredCampaigns, "campaign_id", Seq("campaign_id"), "inner")
        .withColumn("dt", to_date($"created_at"))
        .withColumnRenamed("user_id", "customer_id")
        .addSuperCashBackAggregates(Seq("dt", "customer_id", "campaign_name"))
        .createCashBack(Seq("dt", "customer_id", "campaign_name"), "superCash")
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)

      spark.read.parquet(anchorPath)
    }

    finalSupercashbackData.moveHDFSData(dtSeq, outPath)
  }

  implicit class superCashBackFunc(df: DataFrame) {
    def createCashBack(ignoredColumns: Seq[String], structColName: String): DataFrame = {
      val columns = df.columns.filter(col => !ignoredColumns.contains(col))

      df.withColumn(
        structColName,
        struct(col("campaign_name").alias("campaign_name") +: columns.map(col(_)): _*)
      )
    }
  }
}