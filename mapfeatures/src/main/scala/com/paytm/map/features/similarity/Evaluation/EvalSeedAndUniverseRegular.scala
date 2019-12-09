package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object EvalSeedAndUniverseRegular extends EvalSeedAndUniverse {

  override def generate(
    spark: SparkSession,
    date: DateTime,
    settings: Settings,
    trainingData: String,
    baseData: String,
    testingData: String,
    positiveSampleRatio: Double,
    excludedSegment: Option[DataFrame]
  ): Map[String, DataFrame] = {

    val seedDF = spark.read.parquet(trainingData)
    val universe = excludedSegment match { // to remove some specific group, such as customers only having clicks
      case Some(x) => spark.read.parquet(testingData).join(x, Seq("customer_id"), "left_anti")
      case _       => spark.read.parquet(testingData)
    }

    universe.cache().count()
    val dfMap = Map("seed" -> seedDF, "universe" -> universe)

    dfMap //return

  }

}
