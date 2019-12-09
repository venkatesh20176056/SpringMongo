package com.paytm.map.features.devicepush

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object DeviceIdFeatures extends DeviceIdFeaturesJob with SparkJob with SparkJobBootstrap {

}

trait DeviceIdFeaturesJob {
  this: SparkJob =>

  val JobName: String = "DeviceIdFeatures"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.FormatPattern)

    val userTokenPath = s"${settings.featuresDfs.pushCassandraUserToken}/dt=$targetDateStr"

    val deviceInfoPath = s"${settings.featuresDfs.pushCassandraDeviceInfo}/dt=$targetDateStr"

    val userTokenTable = spark.read.parquet(userTokenPath)

    val deviceInfoTable = spark.read.parquet(deviceInfoPath)

    deviceInfoTable.select(
      $"pushId",
      to_date(from_unixtime($"fcmtoken_writeTime" / 1000000)).alias("first_open_date")
    )
      .join(
        userTokenTable.select(
          $"pushId",
          to_date(from_unixtime($"fcmtoken_writeTime" / 1000000)).alias("signup_date")

        ).groupBy("pushId").agg(max("signup_date").alias("signup_date")),
        Seq("pushId"),
        "full_outer"
      )
      .withColumnRenamed("pushId", "customer_id") //This is a device id, not a customer id
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${settings.featuresDfs.featuresTable}/${settings.featuresDfs.deviceIdPath}dt=$targetDateStr")

  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

}
