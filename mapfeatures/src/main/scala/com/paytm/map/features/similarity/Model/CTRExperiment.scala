package com.paytm.map.features.similarity.Model

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait CTRExperiment {
  def baseCustomerCsvSpecSample(spark: SparkSession, jobDF: DataFrame): DataFrame = {
    import org.apache.hadoop.fs.Path
    import spark.implicits._

    val userBaseFilePrefix = "s3a://clm-production/rule-engine/job/"
    val userBaseFileFS = new Path(userBaseFilePrefix).getFileSystem(spark.sparkContext.hadoopConfiguration)

    jobDF.select($"job_id", $"task_id").rdd.collect()
      .flatMap {
        case Row(job_id: Long, task_id: Long) =>
          val path = s"${userBaseFilePrefix}/${job_id}/${task_id}/*.csv"

          userBaseFileFS.globStatus(new Path(path)).map(_.getPath.toString)
            .map {
              (found_path: String) =>
                val userBaseFileName = FilenameUtils.getName(found_path).stripSuffix(".csv")
                (job_id, task_id, found_path)
            }
      }.toSeq.toDF("job_id", "task_id", "csvpath")
      .filter($"csvpath".isNotNull)
  }

  def sampleCSV(spark: SparkSession, jobPathCSV: String): DataFrame = {
    import spark.implicits._
    val ts = to_timestamp($"datework", "yyyy-MM-dd HH:mm:ss")
    val jobDF = spark.read.option("header", "true").csv(jobPathCSV)
      .select(
        $"job_id".cast(LongType).as("job_id"),
        $"task_id".cast(LongType).as("task_id"),
        $"campaign_id".cast(LongType).as("campaign_id"),
        ts.as("timestamp")
      )
      .withColumn("ISTtimestamp", $"timestamp" + expr("INTERVAL 6 HOURS") + expr("INTERVAL 30 MINUTES"))
      .withColumn("ISTdate", $"ISTtimestamp".cast(DateType).cast(StringType))
      .select("job_id", "task_id", "campaign_id", "ISTdate")

    val pathDF = baseCustomerCsvSpecSample(spark, jobDF)
    val jobPathDF = jobDF.join(pathDF, Seq("job_id", "task_id"))
    jobPathDF
  }

  def getClickInfo(spark: SparkSession, signalInternalPromotionFolder: String, cmaFolder: String, dateStr: String): DataFrame = {
    import spark.implicits._

    val signalInternalPromotionPath = s"$signalInternalPromotionFolder/dt=$dateStr"
    val cmaPath = s"$cmaFolder/dt=$dateStr"
    val signalInternalPromotion = spark.read
      .parquet(signalInternalPromotionPath)
      .withColumn("customer_id", $"customer_id")
      .select($"customer_id", $"impression", $"banner_id", $"click")

    val CMA = spark.read.parquet(cmaPath)
      .select($"banners", $"campaign_id")
      .withColumn("banner_id", explode($"banners"))
      .drop("banners")

    val clickInfo = signalInternalPromotion
      .filter($"banner_id".isNotNull && $"customer_id".isNotNull)
      .join(broadcast(CMA), Seq("banner_id"))
      .select($"customer_id", $"campaign_id", $"banner_id", $"impression", $"click")
    clickInfo
  }

  def getCTRByUser(spark: SparkSession, clickInfo: DataFrame, userDF: DataFrame): DataFrame = {
    import spark.implicits._
    clickInfo
      .join(
        broadcast(userDF.select("customer_id")),
        Seq("customer_id")
      )
      .groupBy("banner_id")
      .agg(sum($"impression") as "impression_count", sum($"click") as "click_count", countDistinct($"customer_id") as "unique_customer_count")
      .select(
        $"banner_id",
        $"impression_count", $"click_count", $"unique_customer_count",
        round($"click_count".cast(LongType) / $"impression_count", 5) * 100.0 as "ctr"
      )
  }

  def writeCTR(spark: SparkSession, userDF: DataFrame, clickInfo: DataFrame,
    date: String, CTRbasePath: String, campaignId: Long) {
    //    val baseCTRpath = s"$CTRpath/type=base/dt=${date}/job_id=${jobId}"
    val CTRpath = s"$CTRbasePath/dt=${date}/campaign_id=${campaignId}"

    val CTR = getCTRByUser(spark, clickInfo, userDF)
    CTR.write.mode("overwrite").parquet(CTRpath)
  }
}
