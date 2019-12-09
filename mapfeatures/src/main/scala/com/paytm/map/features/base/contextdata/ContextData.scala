package com.paytm.map.features.base.contextdata

import ch.hsr.geohash.GeoHash.{geoHashStringWithCharacterPrecision => encodeLatLong}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.sanity.TableState
import com.paytm.map.features.utils.UDFs.gaugeValuesWithTags
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.format.DateTimeFormat

import scala.util.Try

object ContextData extends ContextDataJob
  with SparkJob with SparkJobBootstrap

trait ContextDataJob {
  this: SparkJob =>

  val JobName = "ContextData"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def extractGeoUDF(geohashPrecision: Int = 12) =
    udf((lat: Double, long: Double) => Try(encodeLatLong(lat, long, geohashPrecision)).toOption)

  def generateContextData(
    spark: SparkSession,
    msAuditLog: DataFrame,
    geohashPrecision: Int
  ): DataFrame = {
    import org.apache.spark.sql.types._
    import spark.implicits._

    val requestContextSchema = StructType(Seq(
      StructField("channel", StringType, true),
      StructField("version", StringType, true),
      StructField("device", StructType(Seq(
        StructField("deviceType", StringType, true),
        StructField("aaid", StringType, true),
        StructField("idfa", StringType, true),
        StructField("browserUuid", StringType, true),
        StructField("make", StringType, true),
        StructField("hwv", StringType, true),
        StructField("os", StringType, true),
        StructField("osv", StringType, true),
        StructField("geo", StructType(Seq(
          StructField("lat", StringType, true),
          StructField("long", StringType, true)
        )), true)
      )), true)
    ))

    msAuditLog
      .withColumn("context", from_json($"contexts", requestContextSchema))
      .withColumn("device_id", coalesce($"context.device.aaid", $"context.device.idfa", $"context.device.browserUuid"))
      .withColumn(
        "geohash",
        extractGeoUDF(geohashPrecision)($"context.device.geo.lat".cast("double"), $"context.device.geo.long".cast("double"))
      )
      .select(
        $"responseCustomerId" as "customer_id",
        $"created_ts" as "time",
        $"context.channel",
        $"context.version",
        $"context.device.deviceType",
        $"context.device.make",
        $"context.device.hwv",
        $"context.device.os",
        $"context.device.osv",
        $"context.device.geo.lat",
        $"context.device.geo.long",
        $"device_id",
        $"geohash"
      )
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings._

    val geohashPrecision = settings.contextData.geohashPrecision
    val targetDateTime = ArgsUtils.getTargetDate(args)
    val targetDateString = ArgsUtils.getTargetDateStr(args)
    val StringPartitionDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val partition = StringPartitionDateFormatter.print(targetDateTime)

    val auditLogPath = s"${datalakeDfs.midgarAuditLogs}/dt=$partition"
    val destination = s"${settings.featuresDfs.baseDFS.auditLogsTablePath}/dt=$targetDateString"

    try {
      val auditLog = spark.read.parquet(auditLogPath)

      generateContextData(spark, auditLog, geohashPrecision)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(destination)
    } catch {
      case e: Throwable =>
        log.warn("No data available, skipping today's run", e)
        val metricName = "DataAvailability.status"
        val gaugeValue = 1

        gaugeValuesWithTags(
          metricName,
          Map("table" -> "MS_audit_logs", "state" -> TableState.notIngested),
          gaugeValue
        )
    }
  }
}