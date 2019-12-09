package com.paytm.map.features.campaigns

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.paytm.map.features.campaigns.CampaignConstants._
import org.apache.spark.sql.types.StringType

object UpdateSendgrid extends UpdateSendgridJob with SparkJob with SparkJobBootstrap

trait UpdateSendgridJob {
  this: SparkJob =>

  val JobName = "UpdateSendgrid"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def transformSendgrid(spark: SparkSession, sendgridDf: DataFrame, sendgridCond: Seq[(String, Column, Column)], targetDate: String): DataFrame = {

    import spark.implicits._

    val sendgridCols = sendgridDf
      .withColumn("category0", $"category"(0))
      .withColumn("category1", $"category"(1))
      .withColumn("campaign", when($"category1".like("%paytm%"), $"category1").otherwise($"category0"))
      .withColumn("datetime", from_unixtime($"timestamp").cast("timestamp"))
      .select(
        $"email" as "email_id",
        $"campaign",
        $"datetime",
        $"event"
      )

    sendgridCols
      .filter($"event".isin("delivered", "open"))
      .caseWhen(sendgridCond)
      .groupByAggregate(sendGridGroupBy, campaignBaseAggs)
      .withColumn("country", when(sendgridCanadaCond, "canada").otherwise("india"))
      .withColumn("source", lit("sendgrid"))
      .withColumn("dt", lit(targetDate))
      .drop("campaign")
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings.{featuresDfs, campaignsDfs}

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val ydaySlash = ArgsUtils.getTargetDate(args).toString("YYYY/MM/dd")

    val sgPath = campaignsDfs.sendgridEvents
    val outPath = s"${featuresDfs.campaignsTable}/"

    val sendgridYday = readSendgridData(spark, sgPath, ydaySlash)
    val sendgridTransformed = transformSendgrid(spark, sendgridYday, sendGridConditions, targetDateStr)

    sendgridTransformed.write.mode(SaveMode.Append).partitionBy("country", "source", "dt").parquet(outPath)
  }
}

