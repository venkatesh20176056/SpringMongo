package com.paytm.map.features.sanity

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.monitoring.Monitoring
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs.gaugeValuesWithTag
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CheckFeatureCounts extends CheckFeatureCountsJob with SparkJob with SparkJobBootstrap

trait CheckFeatureCountsJob extends Monitoring {
  this: SparkJob =>

  val JobName = "CheckFeatureCounts"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val groupPrefix = args(1)
    val iDataPath = s"${featuresDfs.measurementPath}/nonNullValues/dt=$targetDateStr"
    val joinedDataPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.joined}/dt=$targetDateStr/".replace("stg", "prod")

    // Constants
    val featureCountMetric = s"feature_count_job.total_nullvalue_customers.group_$groupPrefix"
    val tagName = "feature"
    val trackFeatureNames = Seq("customer_id")

    // Read datasets
    val data = spark.read.parquet(joinedDataPath)
    val schemaFields = data.schema.fields.filter(field => !Seq("dt").contains(field.name))
    val aggColumnNames = schemaFields.map(_.name) :+ "total_count"

    val chunkSize = {
      // run balanced iterations where chunkSize is no more than 2000
      // Tested successfully for 2000 size at a time.
      val maxChunkSize = 2000
      val aggSize = aggColumnNames.length.toDouble
      val noIterations = math.ceil(aggSize / maxChunkSize).toInt
      math.ceil(aggSize / noIterations).toInt
    }
    println(chunkSize, aggColumnNames.length)
    val groupedColumns = aggColumnNames.grouped(chunkSize).toArray

    groupedColumns.zipWithIndex.foreach {
      case (group, i) =>
        val aggColumns =
          schemaFields
            .filter(field => group.contains(field.name))
            .map { field =>
              val fType = field.dataType.typeName
              val name = field.name
              sum(when(fieldTypeChecks(name, fType), 0).otherwise(1)).as(name)
            }

        val nullCounts = data
          .groupBy(lit(1))
          .agg(
            sum(lit(1)).as("total_count"),
            aggColumns: _*
          )

        nullCounts.write.mode(SaveMode.Overwrite)
          .parquet(iDataPath + s"/chunk=$i")

        val savedCountsDF = spark.read.parquet(iDataPath + s"/chunk=$i")
        // Push only features which are marked to be tracked in trackFeatureNames
        val trackFeatures = trackFeatureNames.intersect(savedCountsDF.columns)

        trackFeatures.foreach { featureName =>
          println(featureName)
          val count = savedCountsDF.select(featureName).collect().head.getAs[Long](featureName)
          gaugeValuesWithTag(featureCountMetric, tagName, featureName, count)
        }
    }
  }

  def fieldTypeChecks(colM: String, fieldType: String): Column = {
    // possible Values = (long, double, date, string, array, integer)
    val nullCheck = col(colM).isNull
    val typeWiseCheck = fieldType match {
      case "string"  => trim(col(colM)) === ""
      case "integer" => col(colM) === 0
      case "long"    => col(colM) === 0
      case "double"  => col(colM) === 0
      case "date"    => trim(col(colM)) === ""
      case "array"   => size(col(colM)) === 0
      case _         => col(colM).isNull
    }
    nullCheck || typeWiseCheck
  }
}