package com.paytm.map.features.base

import java.net.URI
import java.sql.Date

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter

object DistanceAndSpeed extends DistanceAndSpeedJob with SparkJob with SparkJobBootstrap

trait DistanceAndSpeedJob {
  this: SparkJob =>

  val JobName = "DistanceAndSpeed"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.FormatPattern)
    val gaBackFillDays: Int = args(1).toInt

    val anchorPath = s"${settings.featuresDfs.baseDFS.anchorPath}/DistanceAndSpeed/dt=$targetDateStr"
    val distanceAndSpeedBasePath = s"${settings.featuresDfs.baseDFS.aggPath}/DistanceAndSpeed/dt=$targetDateStr"

    def dayIterator(start: DateTime, end: DateTime, formatter: DateTimeFormatter): Seq[String] = Iterator.iterate(start)(_.plusDays(1)).takeWhile(x => x.isEqual(end) || x.isBefore(end)).map(_.toString(formatter)).toSeq
    val dtSeq = dayIterator(targetDate.minusDays(gaBackFillDays), targetDate, ArgsUtils.formatter)

    val midgarAuditLogPath = s"${settings.featuresDfs.baseDFS.auditLogsTablePath}"

    val fs = FileSystem.get(new URI(midgarAuditLogPath), spark.sparkContext.hadoopConfiguration)
    val midgarauditlogpaths = dtSeq.map(x => midgarAuditLogPath + "/dt=" + x)
      .filter(path => fs.exists(new Path(s"${path}/_SUCCESS")))

    val midgarAuditLog = spark.read
      .parquet(midgarauditlogpaths: _*)
      .where($"customer_id" =!= 0)
      .where($"lat".isNotNull && (trim($"lat") =!= "") && ($"lat" =!= "NULL") && ($"lat" =!= "null") && $"lat" =!= 0)
      .where($"long".isNotNull && (trim($"long") =!= "") && ($"long" =!= "NULL") && ($"long" =!= "null") && $"long" =!= 0)
      .select($"time", $"customer_id", $"lat", $"long")
      .withColumn("dtAndTime", from_unixtime($"time" / 1000, "yyyyMMdd HH:mm")) // $"time"/1000 is done because $"time" is in milliseconds
      .withColumn("dt", to_date(substring(from_unixtime($"time" / 1000), 0, 10))) // $"time"/1000 is done because $"time" is in milliseconds
      .drop($"time")

    val window = Window.partitionBy($"customer_id").orderBy($"dtAndTime")
    val nextLat = lead("lat", 1).over(window)
    val nextLong = lead("long", 1).over(window)
    val nextDtAndTime = lead("dtAndTime", 1).over(window)

    val dist_km = asin(
      sqrt(
        cos(radians("lat")) *
          cos(radians(nextLat)) *
          pow(sin(radians(($"long" - nextLong) / 2)), 2)
          +
          pow(sin(radians(($"lat" - nextLat) / 2)), 2)
      )
    ) * 6371 * 2

    val time_diff_hrs = (unix_timestamp(nextDtAndTime, "yyyyMMdd HH:mm") - unix_timestamp($"dtAndTime", "yyyyMMdd HH:mm")) / (60 * 60)

    val speed = when(time_diff_hrs > 0.09, (dist_km / time_diff_hrs))
      .otherwise(null)

    val endDt: Date = new java.sql.Date(targetDate.getMillis)
    val startDt = new java.sql.Date(targetDate.minusDays(gaBackFillDays).getMillis)

    midgarAuditLog
      .where($"dt".between(startDt, endDt))
      .select($"customer_id", coalesce(dist_km, lit(0)).as("distance"), coalesce(speed, lit(0)).as("speed"))
      .groupBy($"customer_id")
      .agg(max("distance").as("BK_distance_one_week"), max("speed").as("BK_speed_one_week"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(distanceAndSpeedBasePath)

  }

}
