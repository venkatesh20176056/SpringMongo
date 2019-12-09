package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.Model.{AffinityMethod, LSHMethod}
import com.paytm.map.features.similarity.getOptionArgs
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object PrecisionRecall extends PrecisionRecallJob
  with SparkJob with SparkJobBootstrap

trait PrecisionRecallJob extends LSHMethod with AffinityMethod {
  this: SparkJob =>

  val JobName = "PrecisionRecall"

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

    //val groundTruthUniversePath = s"$basePath/truth"
    val groundTruthUniversePath = optionArgs.getOrElse('ground_truth_universe_path, s"$basePath/truth")
    val precisionRecallResultPath = s"$basePath/result"

    val parameterCombinationSize = optionArgs.getOrElse('param_combination_size, "1").toInt

    val evalUniverse = spark.read.load(groundTruthUniversePath).coalesce(1)

    (1 to parameterCombinationSize).foreach(x => {
      val oldResult = spark.read.load(s"$oldModelResultPath/$x").coalesce(1)
      val newResult = spark.read.load(s"$newModelResultPath/$x").coalesce(1)
      val precisionRecallResult = precisionRecallCal(spark, oldResult, newResult, evalUniverse, 0.1)
      precisionRecallResult.coalesce(1).write.mode("overwrite").parquet(s"$precisionRecallResultPath/$x")
    })

  }

  // precision at K method
  private def precisionCal(
    spark: SparkSession,
    result: DataFrame,
    firstNcustomers: Int,
    totalTrueCount: Int,
    modelStr: String
  ): DataFrame = {

    import spark.implicits._

    result.limit(firstNcustomers)
      .withColumn("scoredCustomer", when($"score".isNotNull, 1).otherwise(0))
      .agg(
        sum("label").as("trueCount"),
        count("customer_id").as("customerCount"),
        sum("scoredCustomer").as("scoredCustomerCount")
      ).withColumn("k", lit(firstNcustomers)).withColumn("model", lit(modelStr))
      .withColumn("precision", $"trueCount" / $"k")
      .withColumn("recall", $"trueCount" / lit(totalTrueCount))

  }

  def precisionRecallCal(
    spark: SparkSession,
    currentModelResult: DataFrame,
    newModelResult: DataFrame,
    evalUniverse: DataFrame,
    splitStep: Double
  ): DataFrame = {

    // using
    /**
     * input:
     * evalUniverse: customer_id, label( 1 or 0)
     * modelResult:customer_id, score (double)
     * splitStep: % of total eval universe customers for precision@K
     *
     */
    import spark.implicits._

    val oldResult: DataFrame = evalUniverse.join(currentModelResult, Seq("customer_id"), "left_outer").orderBy(desc("score")).coalesce(1).cache()
    val newResult = evalUniverse.join(newModelResult, Seq("customer_id"), "left_outer").orderBy(desc("score")).coalesce(1).cache()

    val universeCount = evalUniverse.distinct().count()

    val splitCustomerCountSeq = (0.1 to 1.0 by splitStep).map(x => {
      math.floor(x * universeCount).toInt
    })

    val totalTrueCount = evalUniverse.agg(sum("label").as("totalTrue")).select($"totalTrue".cast(IntegerType)).rdd.collect().map(r => r.getInt(0)).head

    val precisionResults = splitCustomerCountSeq.map(x => {

      val oldPrecision = precisionCal(spark, oldResult, x, totalTrueCount, "old")

      val newPrecision = precisionCal(spark, newResult, x, totalTrueCount, "new")

      oldPrecision.union(newPrecision)

    })
    //val totalCustomerCount = evalUniverse.select("customer_id").distinct().count()

    val precisionResultAppend = precisionResults.reduce(_ union _)
      .withColumn("totalTrueCount", lit(totalTrueCount))

    precisionResultAppend
      //.where($"k" <= lit(totalCustomerCount))
      .groupBy("k")
      .pivot("model")
      .agg(max("precision").as("precision"), max("recall").as("recall")).orderBy($"k")
      .select("k", "new_precision", "new_recall", "old_precision", "old_recall")

    // k, new_precision, new_recall, old_precision, old_recall

    // return

  }

}
