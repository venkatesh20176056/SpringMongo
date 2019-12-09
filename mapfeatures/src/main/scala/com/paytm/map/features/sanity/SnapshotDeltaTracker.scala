package com.paytm.map.features.sanity

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{Row, SparkSession}
import com.paytm.map.features.utils.UDFs.{gaugeValues, gaugeValuesWithTag}
import org.apache.spark.sql.functions._

object SnapshotDeltaTracker extends SnapshotDeltaTrackerJob with SparkJob with SparkJobBootstrap

trait SnapshotDeltaTrackerJob {
  this: SparkJob =>

  val JobName = "SnapshotDeltaTracker"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import spark.implicits._

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val featureSetName = args(1)
    val pathToDelta = featuresDfs.exportTable + s"snapshot_delta/$featureSetName/dt=" + targetDateStr

    // Constants
    val featureCountMetric = s"deltajob.$featureSetName.total_customers_changed"
    val tagName = "feature"
    val totalCountMetric = s"deltajob.$featureSetName.total_fields_changed"

    // Read datasets
    val deltaDF = spark.read.parquet(pathToDelta)

    // WORK
    val featureCounts = deltaDF
      .select("customer_id", "columns_with_change")
      .withColumn("feature_name", explode($"columns_with_change"))
      .drop("columns_with_change")
      .groupBy("feature_name")
      .count

    featureCounts.cache

    val featureCountArray = featureCounts.rdd.map {
      case Row(featureName: String, countOfCustomers: Long) => (featureName, countOfCustomers)
    }.collect
    val totalFieldsChanged = featureCountArray.map(_._2).sum

    // Gauge count of how many times all features changed
    gaugeValues(totalCountMetric, totalFieldsChanged)

    // Gauge count of how many times each feature changed
    featureCountArray.map {
      case (featureName: String, count: Long) =>
        gaugeValuesWithTag(featureCountMetric, tagName, featureName, count)
    }
  }
}