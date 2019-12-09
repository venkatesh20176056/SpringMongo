package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo.LocationSchema
import com.paytm.map.features.utils.UDFs.toOperatorCnt
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LocationFeatures extends LocationFeaturesJob
  with SparkJob with SparkJobBootstrap

trait LocationFeaturesJob {
  this: SparkJob =>

  val JobName = "LocationFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark

    // Get the command line parameters
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    // Parse Path Config
    val aggDataPath = s"${settings.featuresDfs.baseDFS.aggPath}/LocationFeatures"

    locationFeatures(settings, targetDateStr)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$aggDataPath/dt=$targetDateStr")
  }

  def locationFeatures(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val auditLog = s"${settings.featuresDfs.baseDFS.auditLogsTablePath}/dt=$targetDateStr"
    val locationDF = spark.read.parquet(auditLog)
    val precision = 7

    locationDF
      .where($"customer_id" =!= 0)
      .where($"geohash".isNotNull and $"geohash" =!= "s00000000000")
      .withColumn("geohash", substring($"geohash", 0, precision))
      .withColumn("ist", from_utc_timestamp(from_unixtime($"time" / 1000), "Asia/Kolkata"))
      .withColumn("hour", hour($"ist"))
      .withColumn("day_of_week", dayofweek($"ist"))
      .groupBy("customer_id")
      .agg(
        toOperatorCnt(collect_list(when('hour >= 21 or 'hour < 9, $"geohash"))) as "home_geohash_count",
        toOperatorCnt(collect_list(when('day_of_week.isin(2, 3, 4, 5, 6) and 'hour >= 9 and 'hour < 21, $"geohash"))) as "work_geohash_count",
        toOperatorCnt(collect_list(when('day_of_week.isin(2, 3, 4, 5, 6), $"geohash"))) as "weekday_geohash_count",
        toOperatorCnt(collect_list(when('day_of_week.isin(1, 7), $"geohash"))) as "weekend_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 0 and 'hour < 6, $"geohash"))) as "night_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 6 and 'hour < 9, $"geohash"))) as "early_morning_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 9 and 'hour < 12, $"geohash"))) as "late_morning_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 12 and 'hour < 15, $"geohash"))) as "early_afternoon_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 15 and 'hour < 18, $"geohash"))) as "late_afternoon_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 18 and 'hour < 21, $"geohash"))) as "early_evening_geohash_count",
        toOperatorCnt(collect_list(when('hour >= 21 and 'hour < 24, $"geohash"))) as "late_evening_geohash_count",
        toOperatorCnt(collect_list('geohash)) as "all_geohash_count"
      )
      .renameColumns("LOCATION_", Seq("customer_id"))
      .alignSchema(LocationSchema)
  }
}
