package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.FileUtils.getLatestTablePath
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

object CustomerDeviceFeatures extends CustomerDeviceFeaturesJob with SparkJob with SparkJobBootstrap

trait CustomerDeviceFeaturesJob {
  this: SparkJob =>

  val JobName: String = "CustomerDeviceFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr: String = targetDate.toString(ArgsUtils.formatter)

    val userDevicePath = s"${settings.featuresDfs.baseDFS.aggPath}/CustomerDeviceFeatures/dt=$targetDateStr"

    val userTokenPath = getLatestTablePath(s"${settings.featuresDfs.pushCassandraUserToken}", spark, targetDateStr, datePrefix = "dt=").get

    val userTokenTable = spark.read.parquet(userTokenPath)

    val deviceAggPath = getLatestTablePath(settings.featuresDfs.measurementsDeviceAgg, spark, targetDateStr, datePrefix = "dt=").get

    val deviceAggTable = spark.read.parquet(deviceAggPath)

    val userTokenAgg = userTokenTable.select(
      $"userId".alias("customer_id"),
      when($"isLoggedOut" === true, lit(1)).alias("isLoggedOut")
    )
      .groupBy("customer_id")
      .agg(max("isLoggedOut").alias("isLoggedOut"))

    val deviceAggTableFiltered =
      deviceAggTable.filter(
        $"customer_id".isNotNull and ($"last_uninstall_time".isNotNull or $"first_install_time".isNotNull)
      )
        .select(
          $"customer_id".cast(LongType),
          to_date($"last_uninstall_time").alias("last_uninstall_date"),
          to_date($"first_install_time").alias("first_install_date")
        )
        .groupBy("customer_id")
        .agg(
          max("last_uninstall_date").alias("last_uninstall_date"),
          min("first_install_date").alias("first_install_date")
        )

    userTokenAgg
      .join(
        deviceAggTableFiltered,
        Seq("customer_id"),
        "full_outer"
      )
      .addPrefixToColumns("CUSTOMERDEVICE_", Seq("customer_id", "dt"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(userDevicePath)

  }
}

