package com.paytm.map.features.campaigns

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import com.paytm.map.features.campaigns.CampaignConstants._
import org.apache.spark.sql.types.{StringType}

object SendgridHistoricals extends SendgridHistoricalsJob with SparkJob with SparkJobBootstrap

trait SendgridHistoricalsJob {
  this: SparkJob =>

  val JobName = "SendgridHistoricals"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def readSendgridDataAll(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val sendgridDf = spark.read.json(s"$path/*/*/*/*")

    if (sendgridDf.schema("category").dataType == StringType) {
      sendgridDf.withColumn("category", split(regexp_replace($"category", "[\\]\\[\"]", ""), ","))
    } else sendgridDf
  }

  def transformSendgridDt(spark: SparkSession, sendgridDf: DataFrame, sendgridCond: Seq[(String, Column, Column)]): DataFrame = {
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
      .drop("campaign")
      .withColumn("dt", when($"sentTime".isNull, $"openedTime").otherwise($"sentTime"))
      .withColumn("dt", $"dt".cast("date"))
      .withColumn("source", lit("sendgrid"))

  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings.{featuresDfs, campaignsDfs}

    val sgPath = campaignsDfs.sendgridEvents
    val outPathNoDt = s"${featuresDfs.campaignsTable}/"

    val sendgridAll = readSendgridDataAll(spark, sgPath)
    val sendgridTransformedAll = transformSendgridDt(spark, sendgridAll, sendGridConditions)

    sendgridTransformedAll.write.mode("append").partitionBy("country", "source", "dt").parquet(outPathNoDt)
  }
}

