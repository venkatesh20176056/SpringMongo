package com.paytm.map.features.campaigns

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SendgridCanadaCsv extends SendgridCanadaCsvJob with SparkJob with SparkJobBootstrap

trait SendgridCanadaCsvJob {
  this: SparkJob =>

  val JobName = "SendgridCanadaCsv"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings.featuresDfs
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)

    val campaignsPath = s"${featuresDfs.campaignsTable}/country=canada/source=sendgrid/"
    val campaignsCsvPath = s"${featuresDfs.canadaCampaigns}/dt=$targetDateStr"

    val canadaCampaigns = spark.read.parquet(campaignsPath)

    canadaCampaigns.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(campaignsCsvPath)
  }
}

