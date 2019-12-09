package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.{baseDataPrep, getOptionArgs}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{rand, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object ExternalTrainingAndTestingDataSimulator extends ExternalTrainingAndTestingDataSimulatorJob with SparkJob with SparkJobBootstrap

trait ExternalTrainingAndTestingDataSimulatorJob {
  this: SparkJob =>

  val JobName = "ExternalTrainingAndTestingDataSimulator"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    val optionArgs = getOptionArgs(Map(), args.toList)
    val segmentId = optionArgs('segment_id)
    val execTime = optionArgs('execution_timestamp)
    val basePath = s"${settings.featuresDfs.lookAlike}/evaluation/metrics/precision_recall/$segmentId/$execTime"
    val groundTruthUniversePath = optionArgs.getOrElse('ground_truth_universe_path, s"$basePath/truth")
    val seedPath = optionArgs.getOrElse('seed_path, s"$basePath/seed")

    val base = loadBase(spark, targetDate, settings)
    val features = loadFeatureVectors(spark, targetDateStr, settings)

    val datasets: (DataFrame, DataFrame) = genSyntheticDatasets(spark, base, features, targetDateStr)

    datasets._1.coalesce(2).write.mode("overwrite").parquet(seedPath)
    datasets._2.coalesce(2).write.mode("overwrite").parquet(groundTruthUniversePath)
  }

  private def loadBase(
    spark: SparkSession,
    date: DateTime,
    settings: Settings
  ): DataFrame = {

    val offlineCols = Seq(
      "customer_id",
      "OFFLINE_total_transaction_count",
      "OFFLINE_first_transaction_date",
      "last_seen_paytm_app_date",
      "PAYTM_total_transaction_size"
    )

    val rawDF = baseDataPrep(offlineCols, date, settings)(spark).orderBy(rand())

    rawDF

  }

  private def loadFeatureVectors(
    spark: SparkSession,
    dateStr: String,
    settings: Settings
  ): DataFrame = {

    UserTrendFilteringAffinityVector.EvalVectorGeneration(spark, dateStr, settings).limit(10000000)

  }

  private def genSyntheticDatasets(
    spark: SparkSession,
    base: DataFrame,
    features: DataFrame,
    dateStr: String
  ): (DataFrame, DataFrame) = {
    import spark.implicits._

    val universe = base.where(col("OFFLINE_total_transaction_count").equalTo(0))
      .select($"customer_id", $"last_seen_paytm_app_date".as("recency"), $"PAYTM_total_transaction_size".as("txsize"))
    val udf = universe.sample(withReplacement = false, 0.05).join(features, Seq("customer_id")).withColumn("label", lit(0)).cache()
    udf.count()

    val seed = base.withColumn("today", lit(dateStr))
      .withColumn("daysAfterFirstOffline", datediff($"today", $"OFFLINE_first_transaction_date"))
      .where($"daysAfterFirstOffline".lt(90))
      .select($"customer_id", $"last_seen_paytm_app_date".as("recency"), $"PAYTM_total_transaction_size".as("txsize"))
    val sdf = seed.sample(withReplacement = false, 0.05).join(features, Seq("customer_id")).withColumn("label", lit(1)).cache()
    sdf.count()

    val training = sdf.sample(withReplacement = false, 0.7).cache()
    training.count()
    val seedRemainingDF = sdf.except(training)
    val testing = udf.union(seedRemainingDF).cache()
    testing.count()

    (training, testing)

  }

}