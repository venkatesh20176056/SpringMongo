package com.paytm.map.features

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.SparkSession

object Sample extends SampleJob
  with SparkJob with SparkJobBootstrap

trait SampleJob {
  this: SparkJob =>

  val JobName = "Sample"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

  }
}
