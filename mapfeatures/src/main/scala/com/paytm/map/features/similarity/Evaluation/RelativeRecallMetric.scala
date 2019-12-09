package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.getOptionArgs
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{count, lit, max, rand, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RelativeRecallMetric extends RelativeRecallMetricJob with SparkJob with SparkJobBootstrap

trait RelativeRecallMetricJob {

  this: SparkJob =>

  val JobName = "RelativeRecallMetric"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val optionArgs = getOptionArgs(Map(), args.toList)

    val segmentId = optionArgs('segment_id)
    val execTime = optionArgs('execution_timestamp)
    val basePath = s"${settings.featuresDfs.lookAlike}/evaluation/metrics/precision_recall/$segmentId/$execTime"
    val oldModelResultPath = s"$basePath/old"
    val newModelResultPath = s"$basePath/new"
    val groundTruthUniversePath = s"$basePath/truth"
    val recallResultPath = s"$basePath/result"
    val parameterCombinationSize = optionArgs('param_combination_size).toInt
    val groundTruth = spark.read.load(groundTruthUniversePath).select("customer_id").distinct().coalesce(1)

    val fullSeedLSHCount = spark.read.load(s"$basePath/full_seed_lsh").distinct().count()

    (1 to parameterCombinationSize).foreach(x => {
      val oldResult = spark.read.load(s"$oldModelResultPath/$x").select("customer_id").coalesce(1)
      val newResult = spark.read.load(s"$newModelResultPath/$x").select("customer_id").coalesce(1)
      val recallResult = recall(spark, oldResult, newResult, groundTruth).withColumn("fullLSH", lit(fullSeedLSHCount))
      recallResult.coalesce(1).write.mode("overwrite").parquet(s"$recallResultPath/$x")
    })

  }

  def genSplits(universeCount: Long): Seq[Int] = {
    val splits = (0.02 until 0.4 by 0.02) ++ (0.4 until 0.7 by 0.05) ++ (0.7 to 1.0 by 0.1)
    splits.map(x => math.floor(x * universeCount).toInt)
  }

  def recall(spark: SparkSession, oldModelResult: DataFrame, newModelResult: DataFrame, groundTruth: DataFrame): DataFrame = {

    import spark.implicits._

    val universeCount = Math.max(oldModelResult.count(), newModelResult.count())
    val splitCustomerCountSeq: Seq[Int] = genSplits(universeCount)

    val totalTrueCount = groundTruth.count

    val recallResults = splitCustomerCountSeq.map(x => {
      val oldRecallCount: Long = computeRecallCount(spark, oldModelResult, x, groundTruth)
      val newRecallCount: Long = computeRecallCount(spark, newModelResult, x, groundTruth)
      val ratio = if (oldRecallCount == 0) {
        1D * newRecallCount
      } else {
        1D * newRecallCount / oldRecallCount
      }
      val df = Seq((x, oldRecallCount, newRecallCount, ratio)).toDF("N", "old_recall_count", "new_recall_count", "ratio")
      df

    }).reduce(_ union _).withColumn("totalTrueCount", lit(totalTrueCount))

    recallResults

  }

  private def computeRecallCount(spark: SparkSession, result: DataFrame, firstNcustomers: Int, groundTruth: DataFrame): Long = {
    groundTruth.join(result.limit(firstNcustomers), Seq("customer_id")).distinct().count()
  }
}