package com.paytm.map.features.campaigns

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType, StructField, LongType}

object CampaignConstants {

  val campaignGroupBy = Seq("email_id")
  val indiaCampaignMonths = 6
  val canadaCampaignDays = Seq(1, 7, 30)
  val sendgridCanadaCond: Column = col("sentCampaign").like("paytm-canada%") || col("openedCampaign").like("paytm-canada%")
  val campaignRegex = "(paytm-\\w+)_campaign_(\\d+)"

  def readSendgridData(spark: SparkSession, path: String, day: String): DataFrame = {
    import spark.implicits._

    val sendgridDf = spark.read.json(s"$path/$day/*")

    if (sendgridDf.schema("category").dataType == StringType) {
      sendgridDf.withColumn("category", split(regexp_replace($"category", "[\\]\\[\"]", ""), ","))
    } else sendgridDf
  }

  def getCampaignFilters(days: Int, targetDate: String): Seq[String] = {
    Seq(s"openedTime > date_sub(to_date('$targetDate'), $days)")
  }

  val betaConditions = Seq(
    ("sentCampaign", col("sentTime").isNotNull, col("emailCampaignId")),
    ("openedCampaign", col("openedTime").isNotNull, col("emailCampaignId"))
  )

  val sendGridConditions = Seq(
    ("sentTime", col("event") === "delivered", col("datetime")),
    ("openedTime", col("event") === "open", col("datetime")),
    ("sentCampaign", col("event") === "delivered", col("campaign")),
    ("openedCampaign", col("event") === "open", col("campaign"))
  )

  val sendGridGroupBy = Seq("campaign", "email_id")

  val betaoutGroupBy = Seq("emailCampaignId", "email_id")

  val campaignBaseAggs = Seq(
    ("min", "sentTime         as   sentTime"),
    ("max", "openedTime       as   openedTime"),
    ("max", "sentCampaign     as   sentCampaign"),
    ("max", "openedCampaign   as   openedCampaign")
  )

  val campaignMaxAggs = Seq(
    ("max", "sentTime        as  latest_delivery_date"),
    ("max", "openedTime      as  latest_open_date")
  )

  def getCampaignCountAggs(months: Int): Seq[(String, String)] = {
    Seq(
      ("count", s"openedTime  as  campaigns_opened_${months}_months"),
      ("collect_as_set", s"campaign_id as campaign_ids_opened_${months}_months")
    )
  }

  def getCampaignCountDaysAggs(days: Int): Seq[(String, String)] = {
    Seq(
      ("count_distinct", s"openedCampaign  as  campaigns_opened_${days}_days")
    )
  }

  val customerCsvSchema = StructType(Seq(StructField("customer_id", LongType, true)))
}

