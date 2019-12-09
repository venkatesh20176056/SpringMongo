package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import com.paytm.map.features.base.DataTables._

object GATravelAgg extends GATravelAggJob
  with SparkJob with SparkJobBootstrap

trait GATravelAggJob {

  this: SparkJob =>

  val JobName = "GATravelAgg"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val gaBackFillDays: Int = args(1).toInt
    val anchorPath = s"${settings.featuresDfs.baseDFS.anchorPath}/GATravel"
    val gaTravelBasePath = s"${settings.featuresDfs.baseDFS.aggPath}/GATravel"

    def dayIterator(start: DateTime, end: DateTime, formatter: DateTimeFormatter): Seq[String] = Iterator.iterate(start)(_.plusDays(1)).takeWhile(x => x.isEqual(end) || x.isBefore(end)).map(_.toString(formatter)).toSeq
    val dtSeq = dayIterator(targetDate.minusDays(gaBackFillDays), targetDate, ArgsUtils.formatter)

    val midgarAuditLogPath = s"${settings.featuresDfs.baseDFS.auditLogsTablePath}"
    val midgarauditlogpaths = dtSeq.map(x => midgarAuditLogPath + "/dt=" + x)

    val midgarAuditLog = spark.read
      .parquet(midgarauditlogpaths: _*)
      .where($"customer_id" =!= 0)
      .where($"lat".isNotNull && (trim($"lat") =!= "") && ($"lat" =!= "NULL") && ($"lat" =!= "null") && $"lat" =!= 0)
      .where($"long".isNotNull && (trim($"long") =!= "") && ($"long" =!= "NULL") && ($"long" =!= "null") && $"long" =!= 0)
      .select($"time", $"customer_id", $"lat", $"long")
      .withColumn("dt", to_date(substring(from_unixtime($"time" / 1000), 0, 10))) // $"time"/1000 is done because $"time" is in milliseconds
      .drop($"time")

    val window = Window.partitionBy($"customer_id").orderBy($"dt")
    val nextLat = lead("lat", 1).over(window)
    val nextLong = lead("long", 1).over(window)
    val nextDt = lead("dt", 1).over(window)

    val dist_km = asin(
      sqrt(
        cos(radians("lat")) *
          cos(radians(nextLat)) *
          pow(sin(radians(($"long" - nextLong) / 2)), 2)
          +
          pow(sin(radians(($"lat" - nextLat) / 2)), 2)
      )
    ) * 6371 * 2

    val time_diff_hrs = (unix_timestamp(nextDt, "yyyyMMdd HH:mm") - unix_timestamp($"dt", "yyyyMMdd HH:mm")) / (60 * 60)

    val speed = dist_km / time_diff_hrs

    val endDt = new java.sql.Date(targetDate.getMillis)
    val startDt = new java.sql.Date(targetDate.minusDays(gaBackFillDays).getMillis)

    val distanceAndSpeed = {
      midgarAuditLog
        .where($"dt".between(startDt, endDt))
        .select($"customer_id", $"dt", coalesce(dist_km, lit(0)).as("distance"), coalesce(speed, lit(0)).as("speed"))
        .groupBy($"customer_id", $"dt")
        .agg(max("distance").as("distance"), max("speed").as("speed"))
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    distanceAndSpeed.moveHDFSData(dtSeq, gaTravelBasePath)

  }

}
