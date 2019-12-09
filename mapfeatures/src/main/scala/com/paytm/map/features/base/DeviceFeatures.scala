package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo.DeviceSchema
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DeviceFeatures extends DeviceFeaturesJob
  with SparkJob with SparkJobBootstrap

trait DeviceFeaturesJob {
  this: SparkJob =>

  val JobName = "DeviceFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark

    // Get the command line parameters
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    // Parse Path Config
    val aggDataPath = s"${settings.featuresDfs.baseDFS.aggPath}/DeviceFeatures"

    deviceFeatures(settings, targetDateStr)
      .join(cellNetworkProvider(settings, targetDateStr), Seq("customer_id"), "left_outer")
      .renameColumns("DEVICE_", Seq("customer_id", "installed_apps_list"))
      .alignSchema(DeviceSchema)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$aggDataPath/dt=$targetDateStr")
  }

  def deviceFeatures(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val signalPath = s"${settings.featuresDfs.baseDFS.deviceSignalPath}/dt=$targetDateStr"
    val deviceSignalDF = spark
      .read
      .parquet(signalPath)

    val getAppsList =
      split(
        regexp_replace(
          coalesce(get_json_object($"payload", "$.apps"), get_json_object($"payload", "$.nameValuePairs.apps.values")),
          "\"|\\[|\\]", ""
        ), ","
      )

    deviceSignalDF
      .where($"eventType" === "installed_apps" or
        ($"eventType" === "customEvent" and get_json_object($"payload", "$.event_category") === "app_open"))
      .withColumnRenamed("customerId", "customer_id")
      .withColumn("eventDate", to_date(from_unixtime($"deviceDateTime" / 1000)))
      .groupBy($"customer_id")
      .agg(
        first(getAppsList, true) as "installed_apps_list",
        sum(when($"eventType" === "customEvent", 1).otherwise(0)) as "total_app_open_count",
        max(when($"eventType" === "customEvent" and $"eventDate" <= targetDateStr, $"eventDate").otherwise(null)) as "last_app_open_date"
      )
      .select(
        $"customer_id",
        $"installed_apps_list",
        $"total_app_open_count",
        $"last_app_open_date"
      )
  }

  def cellNetworkProvider(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val cardinalLimit = 100
    val signalPath = s"${settings.featuresDfs.baseDFS.deviceSignalPath}/dt=$targetDateStr"
    val deviceSignalDF = spark
      .read
      .parquet(signalPath)

    val networks = deviceSignalDF
      .where($"eventType" === "customEvent" and get_json_object($"payload", "$.event_category") === "app_open")
      .withColumnRenamed("customerId", "customer_id")
      .withColumn("cell_network_provider", lower(trim(regexp_extract(get_json_object($"payload", "$.event_label"), "network_provider=(.+);", 1))))
      .where($"cell_network_provider".isNotNull and 'cell_network_provider =!= "")
      .withColumn(
        "cell_network_provider_standardized",
        when('cell_network_provider like "%jio%", "jio")
          .when('cell_network_provider like "%vodafone%", "vodafone")
          .when('cell_network_provider like "%airtel%", "airtel")
          .when(('cell_network_provider like "%idea%") or ('cell_network_provider like "%!dea%"), "idea")
          .when('cell_network_provider like "%bsnl%", "bsnl")
          .otherwise('cell_network_provider)
      )
      .select($"customer_id", $"cell_network_provider_standardized" as "cell_network_provider")
      .cache

    val networksValues = networks
      .groupBy("cell_network_provider")
      .count
      .orderBy('count.desc)
      .limit(cardinalLimit)

    networks
      .join(broadcast(networksValues), Seq("cell_network_provider"), "left_semi")
      .groupBy($"customer_id")
      .agg(
        last(networks("cell_network_provider"), true) as "cell_network_provider"
      )
      .select(
        $"customer_id",
        $"cell_network_provider"
      )
  }

}
