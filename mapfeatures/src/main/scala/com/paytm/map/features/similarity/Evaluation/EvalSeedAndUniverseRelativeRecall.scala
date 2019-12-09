package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object EvalSeedAndUniverseRelativeRecall extends EvalSeedAndUniverse {

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

    import spark.implicits._

    val universe = excludedSegment match {
      case Some(y) => spark.read.load(baseData).distinct().join(y, Seq("customer_id"), "left_anti")
      case None    => spark.read.load(baseData)
    }

    val base = loadBase(spark, date, settings)
    val seed = spark.read.load(trainingData).distinct()

    val seedDF = if (seed.columns.toSet.contains("recency")) {
      seed
    } else {
      seed.join(base, Seq("customer_id"))
        .select($"customer_id", $"last_seen_paytm_app_date".as("recency"), $"PAYTM_total_transaction_size".as("txsize"))
      //.groupBy("customer_id")
      //.agg(max("recency_date").as("recency"), max($"tx_size").as("txsize"))
    }

    val trainingDF = seedDF.sample(withReplacement = false, positiveSampleRatio).cache
    val testingDF = seedDF.except(trainingDF)

    testingDF.coalesce(1).write.mode("overwrite").parquet(testingData) /// used in RelativeRecallMetricJob
    trainingDF.coalesce(1).write.mode("overwrite").parquet(s"$trainingData/training")

    val universeDF = if (universe.columns.toSet.contains("recency")) {
      universe.join(trainingDF, Seq("customer_id"), "left_anti").distinct()
    } else {
      universe.join(base, Seq("customer_id"))
        .select($"customer_id", $"last_seen_paytm_app_date".as("recency"), $"PAYTM_total_transaction_size".as("txsize"))
        .join(trainingDF, Seq("customer_id"), "left_anti")
        .distinct()
      //.groupBy("customer_id")
      //.agg(max("recency_date").as("recency"), max($"tx_size").as("txsize"))
    }

    val universeWithTestingDF = universeDF.union(testingDF)

    val dfMap = Map("seed" -> trainingDF, "universe" -> universeWithTestingDF, "seedAll" -> seedDF, "uninverseAll" -> universeDF) // here, we use whole population as universe

    dfMap
  }

}
