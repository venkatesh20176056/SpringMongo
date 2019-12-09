package com.paytm.map.features.featurevalue

import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.featurevalue.FeatureValueConstant._
import org.apache.spark.sql.SparkSession

object FeatureValueJob extends FeatureValue with SparkJob with SparkJobBootstrap

trait FeatureValue {
  this: SparkJob =>

  val JobName = "FeatureValue"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Inputs
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val tenant = args(1)

    val config: FeatureValueConfig = tenant match {
      case "merchant" => FeatureValueMerchant
      case "hero"     => FeatureValueHero
      case "india"    => FeatureValueIndia
      case _          => throw new RuntimeException("Tenant not supported")
    }

    val inputPaths = config.getInputPaths(settings, targetDateStr)

    // Outputs
    val featureValuesOutPath = config.getOutputPath(settings)
    val outPath = s"$featureValuesOutPath/dt=$targetDateStr/values.txt"
    val outTempPath = s"$featureValuesOutPath/dt=$targetDateStr/valuesPartitioned"

    // WORK
    // Read input paths
    val features = readPathsAll(spark, inputPaths).unionAll

    // Get possible values for each column type defined in constants file
    val finalFileContent = colTypesToInclude.flatMap {
      featureColumn: FeatureColumn =>
        val cols = features.getColumnsNamesOfType(featureColumn.dataType).diff(config.getColumnsToExclude)
        featureColumn.getPossibleValues(features, cols)
    }

    val finalRDD = spark.sparkContext.parallelize(finalFileContent, 200)
    finalRDD.saveAsTextFile(outTempPath)

    spark.read.textFile(outTempPath).coalesce(1).rdd.saveAsTextFile(outPath)

  }
}

