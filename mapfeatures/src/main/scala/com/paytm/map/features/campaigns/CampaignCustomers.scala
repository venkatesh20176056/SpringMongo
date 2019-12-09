package com.paytm.map.features.campaigns

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.paytm.map.features.campaigns.CampaignConstants._

object CampaignCustomers extends CampaignCustomersJob with SparkJob with SparkJobBootstrap

trait CampaignCustomersJob {
  this: SparkJob =>

  val JobName = "CampaignCustomers"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    import settings.{featuresDfs, campaignsDfs, datalakeDfs}

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val targetDateSlash = ArgsUtils.getTargetDate(args).toString("YYYY/MM/dd")

    val sgPath = campaignsDfs.sendgridEvents
    val outPath = s"${featuresDfs.campaignCustTable}/"

    val sendgridEvents = readSendgridData(spark, sgPath, targetDateSlash)
    val custReg = readTableV3(spark, datalakeDfs.custRegnPath, targetDateStr)
      .select($"customer_registrationID" as "customer_id", $"customer_email" as "email_id")

    val eventsCampaigns = getCampaignInfo(spark, campaignRegex, sendgridEvents)
    val custCampaignsYday = getCustomerCampaigns(spark, eventsCampaigns, custReg)

    val customerCampaigns = try {
      val previousCustomerCampaigns = spark.read.schema(customerCsvSchema).csv(outPath)
      val columnOrder = previousCustomerCampaigns.columns.map(col)

      custCampaignsYday
        .union(previousCustomerCampaigns.select(columnOrder: _*))
        .distinct
    } catch {
      case e: org.apache.spark.sql.AnalysisException => custCampaignsYday
    }

    customerCampaigns
      .repartition($"tenant", $"campaign_id", $"event")
      .write
      .partitionBy("tenant", "campaign_id", "event")
      .mode(SaveMode.Overwrite)
      .csv(outPath)
  }

  def getCampaignInfo(spark: SparkSession, campaignRegex: String, sendgridDf: DataFrame): DataFrame = {
    import spark.implicits._

    sendgridDf
      .withColumn("category0", $"category"(0))
      .withColumn("category1", $"category"(1))
      .withColumn("campaign", when($"category1".like("%paytm%"), $"category1").otherwise($"category0"))
      .withColumn("tenant", regexp_extract($"campaign", campaignRegex, 1))
      .withColumn("campaign_id", regexp_extract($"campaign", campaignRegex, 2).cast("integer"))
      .withColumn("datetime", from_unixtime($"timestamp").cast("timestamp"))
      .filter($"tenant".isNotNull)
      .select(
        $"email" as "email_id",
        $"campaign_id",
        $"datetime",
        $"event",
        $"tenant"
      )
  }

  def getCustomerCampaigns(spark: SparkSession, sendgridCampaigns: DataFrame, customerIds: DataFrame): DataFrame = {
    import spark.implicits._

    sendgridCampaigns
      .filter($"event".isin("delivered", "open"))
      .join(customerIds, Seq("email_id"))
      .select("tenant", "campaign_id", "event", "customer_id")
      .distinct
  }
}

