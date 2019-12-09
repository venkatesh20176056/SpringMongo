package com.paytm.map.features.campaigns
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import com.paytm.map.features.campaigns.CampaignConstants._
import org.apache.spark.sql.types.{TimestampType, StructType, StringType, StructField}
import org.joda.time.format.DateTimeFormat

object UpdateBetaoutHistoricals extends UpdateBetaoutHistoricalsJob with SparkJob with SparkJobBootstrap

trait UpdateBetaoutHistoricalsJob {
  this: SparkJob =>

  val JobName = "UpdateBetaoutHistoricals"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val NUM_CSVS = 1
    val csvDays = args(NUM_CSVS).toInt

    import spark.implicits._
    import settings.{campaignsDfs, featuresDfs}

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val outPathBetaHistorical = s"${featuresDfs.campaignsTable}/country=india/source=betaout/"

    val betaExtractedPath = s"s3a://${campaignsDfs.betaoutEvents}/email_raw_data_16jan_to_26feb_extracted"
    val betaPathMain = campaignsDfs.betaoutEvents
    val betaRawdataPath = s"${campaignsDfs.betaoutEvents}/email_rawdata"
    val betaRawdataOldPath = s"${campaignsDfs.betaoutEvents}/email_rawdata_old"
    val startDate = "2018-02-26"

    def groupBeta(df: DataFrame): DataFrame = {
      df
        .caseWhen(betaConditions)
        .groupByAggregate(betaoutGroupBy, campaignBaseAggs)
        .drop("emailCampaignId")
        .withColumn("dt", when($"sentTime".isNull, $"openedTime").otherwise($"sentTime"))
        .withColumn("dt", $"dt".cast("date"))
    }

    val betaOldSchema = StructType(Array(
      StructField("emailCampaignId", StringType, true),
      StructField("email_id", StringType, true),
      StructField("status", StringType, true),
      StructField("sentTime", StringType, true),
      StructField("openedTime", StringType, true),
      StructField("clickedTime", StringType, true)
    ))

    /**
     * Get "email_rawdata" historicals
     */
    //    configureSparkForSingaporeS3Access(spark)

    val betaoutRawdata = spark.read.option("header", "true")
      .csv(s"$betaRawdataPath/*/")
      .withColumnRenamed("email", "email_id")
      .withColumn("sentTime", $"sentTime".cast(TimestampType))
      .withColumn("openedTime", $"openedTime".cast(TimestampType))
      .drop("clickedTime", "status")

    val betaoutRawDataSummary = groupBeta(betaoutRawdata).cache()

    configureSparkForMumbaiS3Access(spark)
    betaoutRawDataSummary.write.mode("append").partitionBy("dt").parquet(outPathBetaHistorical)

    /**
     * Get "email_rawdata_old" historicals. No header so schema required.
     */
    configureSparkForSingaporeS3Access(spark)

    val betaoutRawdataOld = spark.read.format("csv")
      .option("header", "true")
      .schema(betaOldSchema)
      .load(s"$betaRawdataOldPath/")
      .withColumn("sentTime", $"sentTime".cast(TimestampType))
      .withColumn("openedTime", $"openedTime".cast(TimestampType))
      .drop("status", "clickedTime")

    val betaoutRawdataOldSummary = groupBeta(betaoutRawdataOld).cache()

    configureSparkForMumbaiS3Access(spark)
    betaoutRawdataOldSummary.write.mode("append").partitionBy("dt").parquet(outPathBetaHistorical)

    /**
     * Get recent CSVs
     */
    configureSparkForSingaporeS3Access(spark)

    val csvs = (0 to csvDays) map { i =>
      val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val startDateTime = dateFormatter.parseDateTime(startDate)
      startDateTime.plusDays(i).toString("YYYY-MM-dd") + ".csv"
    }

    val betaoutCsvs = csvs
      .map(csv => spark.read.option("header", "true").csv(s"$betaPathMain/$csv"))
      .reduce(_ union _)
      .select($"campaign_id" as "emailCampaignId", $"email_id", $"sent_time" as "sentTime", $"opened_time" as "openedTime")
      .withColumn("sentTime", from_unixtime($"sentTime" / 1000).cast("timestamp"))
      .withColumn("openedTime", from_unixtime($"openedTime" / 1000).cast("timestamp"))

    val betaoutCsvsSummary = groupBeta(betaoutCsvs).cache()

    configureSparkForMumbaiS3Access(spark)
    betaoutCsvsSummary.write.mode("append").partitionBy("dt").parquet(outPathBetaHistorical)

    /**
     * Get Jan 16 - feb 26 extracted zip file
     */

    configureSparkForSingaporeS3Access(spark)
    val betaoutExtracted = spark.read.format("csv")
      .option("header", "true")
      .schema(betaOldSchema)
      .load(s"$betaExtractedPath/")
      .withColumnRenamed("email", "email_id")
      .withColumn("sentTime", $"sentTime".cast(TimestampType))
      .withColumn("openedTime", $"openedTime".cast(TimestampType))
      .drop("status", "clickedTime")

    val betaoutExtractedSummary = groupBeta(betaoutExtracted).cache()

    configureSparkForMumbaiS3Access(spark)
    betaoutExtractedSummary.write.mode("append").partitionBy("dt").parquet(outPathBetaHistorical)

  }
}