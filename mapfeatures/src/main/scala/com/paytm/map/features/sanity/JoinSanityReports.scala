package com.paytm.map.features.sanity

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.datasets.Constants.wallet
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs.readPathsAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SaveMode

object JoinSanityReports extends JoinSanityReportsJob with SparkJob with SparkJobBootstrap

trait JoinSanityReportsJob {
  this: SparkJob =>

  val JobName = "JoinSanityReports"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Inputs
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)

    val measurementBasePath = s"${settings.featuresDfs.measurementPath}"

    val sanityReportLevelsPath = measurementBasePath + s"sanity_reports/levels/"
    val sanityReportAggPath = measurementBasePath + s"sanity_reports/final/"
    val levels = Seq("RU", "BK", "EC1", "EC2", wallet.toUpperCase)

    val pathsInArgs = levels.map(level => s"$sanityReportLevelsPath/$level/dt=$targetDateStr")
    val DFArray = readPathsAll(spark, pathsInArgs)

    val aggDf = DFArray.reduce(_ union _)
    val aggDfSorted = aggDf.sort(desc("FailCounts"))

    aggDfSorted.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(sanityReportAggPath + "dt=" + targetDateStr)

  }
}