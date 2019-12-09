package com.paytm.map.features.datasets

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.paytm.map.features.datasets.ReadBaseTable._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import com.paytm.map.features.datasets.Constants._

object CustomerSegmentation extends CustomerSegmentationJob with SparkJob with SparkJobBootstrap

trait CustomerSegmentationJob {
  this: SparkJob =>

  val JobName = "CustomerSegmentation"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val dates = segmentationVal.map(i => targetDate.minusDays(i * 30).toString(ArgsUtils.formatter))

    val dateCol = col("dt")

    val baseTablePath = s"${settings.featuresDfs.baseDFS.aggPath}/"
    val profilePath = featuresDfs.featuresTable + featuresDfs.profile + s"dt=$targetDateStr"

    val base = readBaseTable(spark, baseTablePath, "L1", targetDate, ArgsUtils.formatter.parseDateTime(dates.last))
      .select($"customer_id", dateCol)
      .union(
        readBaseTable(spark, baseTablePath, "GAFeatures/L1", targetDate, ArgsUtils.formatter.parseDateTime(dates.last))
          .select($"customer_id", dateCol)
      )
      .groupBy("customer_id").agg(max(dateCol) as "dt")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val groupsDF = Array.
      tabulate(segmentationVal.length - 1) { i =>
        val out = base.filter(dateCol between (dates(i + 1), dates(i))).
          select($"customer_id")
        out.saveParquet(featuresDfs.featuresTable + s"${i + 1}/" + $"ids/dt=$targetDateStr")
        out
      }

    val activeIds: DataFrame = groupsDF.reduce(_ union _).distinct()
    val profileDF = spark.read.parquet(profilePath)
    val dormantIds = profileDF.join(activeIds, getKey, "left_anti")
    dormantIds.saveParquet(s"${featuresDfs.featuresTable}/${groupList.last}/ids/dt=$targetDateStr")

    // Write Active customer set to CSV - MAP-232
    val activeUserDays = 60
    val startDate = targetDate.minusDays(activeUserDays).toString(ArgsUtils.formatter)
    val outCSVPath = s"${featuresDfs.featuresTable}/1/ids_csv/dt=$targetDateStr"
    base
      .where(dateCol.between(startDate, targetDateStr))
      .select("customer_id")
      .saveCSV(outCSVPath, 1)

  }
}
