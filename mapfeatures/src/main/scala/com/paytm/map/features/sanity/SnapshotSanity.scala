package com.paytm.map.features.sanity

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.datasets.ReadBaseTable._
import org.apache.spark.sql.SparkSession
import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.sanity.SanityConstants._
import org.apache.spark.sql.SaveMode

object SnapshotSanity extends SnapshotSanityJob with SparkJob with SparkJobBootstrap

trait SnapshotSanityJob {
  this: SparkJob =>

  val JobName = "SnapshotSanity"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Inputs
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val levelPrefix = args(1).toUpperCase
    val groupPrefix = args(2)

    val joinedSnapshotPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.joined}".replace("/stg/", "/prod/") + "dt=" + targetDateStr

    val measurementBasePath = s"${settings.featuresDfs.measurementPath}"

    val sanityReportLevelPath = measurementBasePath + s"sanity_reports/levels/$levelPrefix/dt=$targetDateStr"
    val sanityReportTestDfPath = measurementBasePath + s"sanity_reports/raw/$levelPrefix/dt=$targetDateStr"

    println("JoindSnapshotPath : " + joinedSnapshotPath)
    println("measurementBasePath : " + measurementBasePath)

    val defaultMinDateStr = defaultMinDate.toString("YYYY-MM-dd")

    val joinedDf = spark.read.parquet(joinedSnapshotPath)

    // TODO: Add other verticals here
    val verticals = levelPrefix match {
      case "BK"     => l3BK ++ Seq("BK")
      case "EC1"    => l3EC1
      case "EC2"    => l3EC2_I ++ l3EC2_II
      case "WALLET" => Seq(walletLevelPrefix)
      case _        => l3RU
    }

    val availableVerticals = verticals.filter { verticalPrefix =>
      joinedDf.columns.exists(_.contains(verticalPrefix.split("_").drop(1).mkString("_"))) //to do[Done]: better strg split("_").last
    }

    val allTestAggregates = getAllTestAggAll(availableVerticals)
    val reqdCols = allTestAggregates.flatMap(_.baseColumn) ++ Seq("customer_id")
    val uniqueReqdCols = reqdCols.distinct.diff(Seq("today_date", "default_min_date"))

    println(uniqueReqdCols)

    val joinedDfSub = joinedDf.select(uniqueReqdCols.head, uniqueReqdCols.tail: _*)
      .withColumn("default_min_date", lit(defaultMinDateStr))
      .withColumn("today_date", lit(targetDateStr))
      .repartition(600)

    joinedDfSub.cache()

    val testDf_andEvaluation = ParseSanityFeatConfig
      .executeFeatures(allTestAggregates, joinedDfSub, targetDateStr)

    val testDf = testDf_andEvaluation._1

    testDf.write.mode(SaveMode.Overwrite).parquet(sanityReportTestDfPath)

    val testEvaluationMetrics = testDf_andEvaluation._2

    val testEvaluationMetricsDf = testEvaluationMetrics.map(x => (x._1, x._2, x._3, x._4, x._5._1, x._5._2, x._5._3, x._6)).toDF(
      "TestCaseCode",
      "TestCaseName",
      "TestOperator",
      "TestOperands",
      "PassCounts",
      "FailCounts",
      "FailedCustIds",
      "TestTolerance"
    )
      .withColumn("Vertical", lit(levelPrefix))
      .withColumn("State", getState($"PassCounts", $"FailCounts", $"TestTolerance"))

    testEvaluationMetricsDf.write.mode(SaveMode.Overwrite).parquet(sanityReportLevelPath)
  }
}