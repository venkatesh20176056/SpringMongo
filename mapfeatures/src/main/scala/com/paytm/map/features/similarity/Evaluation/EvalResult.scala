package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.Model.{AffinityMethod, LSHMethod}
import com.paytm.map.features.similarity.getOptionArgs
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EvalResult extends EvalResultJob
  with SparkJob with SparkJobBootstrap

trait EvalResultJob extends LSHMethod with AffinityMethod {
  this: SparkJob =>

  val JobName = "EvalResult"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    /**
     * input: affinity, seed, negativeUniverse, positiveSampleRatio
     * output: newModelResult, oldModelResult ( both: customer_id, score), groundTruthUniverse
     */

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    val optionArgs = getOptionArgs(Map(), args.toList)
    val segmentId = optionArgs('segment_id)
    val execTime = optionArgs('execution_timestamp)
    val basePath = s"${settings.featuresDfs.lookAlike}/evaluation/metrics/precision_recall/$segmentId/$execTime"
    val oldModelResultPath = s"$basePath/old"
    val newModelResultPath = s"$basePath/new"

    val groundTruthUniversePath = optionArgs.getOrElse('ground_truth_universe_path, s"$basePath/truth")
    val seedPath = optionArgs.getOrElse('seed_path, s"$basePath/seed")

    val baseDataPath = optionArgs.getOrElse('basedata_path, s"$basePath/base")
    val positiveSampleRatio = optionArgs.getOrElse('positive_sample_ratio, "0.7").toDouble // double such as 0.7

    val oldModelName = optionArgs('old_model_name)
    val newModelName = optionArgs('new_model_name)

    val oldModelParam = optionArgs.getOrElse('old_model_param, "concat_n:5,distanceThreshold:0.45,kendallThreshold:0.1")
    val newModelParam = optionArgs.getOrElse('new_model_param, "concat_n:5,distanceThreshold:0.45,kendallThreshold:0.1")

    val oldModelVector = optionArgs.getOrElse('old_model_vector, "na")
    val newModelVector = optionArgs.getOrElse('new_model_vector, "na")

    val seedUniverseType = optionArgs.getOrElse('eval_seed_universe_type, "offline") // such as offline
    val removeCustomersOnyHavingClicks = optionArgs.getOrElse('rm_customers_only_clicks, "na")
    val parameterCombinationSize = optionArgs.getOrElse('param_combination_size, "1").toInt
    val pooling = optionArgs.getOrElse('pooling, "mean")

    //logger.info("old model: " + oldModelName + ", new model: " + newModelName + ", oldModelVector: " + oldModelVector + ", newModelVector: " + newModelVector + ", seedUniverseType: " + seedUniverseType)

    val vectorMap: Map[String, UserVector] = Map(
      "affinity" -> UserRegularAffinityVector,
      "trendfiltering" -> UserTrendFilteringAffinityVector
    )

    val seedUniverseMap: Map[String, EvalSeedAndUniverse] = Map(
      "offline" -> EvalSeedAndUniverseOffline,
      "regular" -> EvalSeedAndUniverseRegular
    )

    // old,newVectorDF : user vector dataframe such as cateogory affinity

    val affinityScoreDate = ArgsUtils.formatter.parseDateTime(targetDateStr)

    val oldVectorDF = loadFeatureVector(vectorMap, oldModelVector, spark, targetDateStr, settings)
    val newVectorDF = loadFeatureVector(vectorMap, newModelVector, spark, targetDateStr, settings)

    val customersOnlyHavingClicks = removeCustomersOnyHavingClicks match {
      case "yes" => Some(UserTrendFilteringAffinityVector.getCustomersOnlyHavingClicks(spark, targetDate, settings))
      case _     => None
    }

    val seedUniverseMapResult = seedUniverseMap.getOrElse(seedUniverseType, EvalSeedAndUniverseOffline)
      .generate(spark, affinityScoreDate, settings, seedPath, baseDataPath, groundTruthUniversePath, positiveSampleRatio, customersOnlyHavingClicks)

    val seedDF: DataFrame = seedUniverseMapResult("seed").distinct()
    val universeDF: DataFrame = seedUniverseMapResult("universe").distinct()

    // parameter provided by command args
    val oldParams: Seq[Map[String, String]] = parseParam(oldModelParam)
    val newParams: Seq[Map[String, String]] = parseParam(newModelParam)

    // given model name can choose model method and parameters within modelResult method
    val oldResults: Seq[DataFrame] = modelResults(spark, seedDF, universeDF, oldVectorDF, oldModelName, oldParams, pooling)
    val newResults: Seq[DataFrame] = modelResults(spark, seedDF, universeDF, newVectorDF, newModelName, newParams, pooling)

    assert(parameterCombinationSize == oldResults.size, "parameterCombinationSize != oldResults.size") // should same
    assert(parameterCombinationSize == newResults.size, "parameterCombinationSize != newResults.size") // should same

    for ((index, df) <- (1 to parameterCombinationSize) zip oldResults) {
      df.coalesce(1).write.mode("overwrite").parquet(s"$oldModelResultPath/$index")
    }

    for ((index, df) <- (1 to parameterCombinationSize) zip newResults) {
      df.coalesce(1).write.mode("overwrite").parquet(s"$newModelResultPath/$index")
    }
  }

  def parseParam(paramStr: String): Seq[Map[String, String]] = {
    val items: Seq[String] = paramStr.split("-").toSeq
    items.map(x => parseParamToMap(x))
  }

  // param to be parse and convert to map
  def parseParamToMap(paramStr: String): Map[String, String] = {

    val commaSpliter = paramStr.split(",") //Array
    val colonSpliter = commaSpliter.map(x => {
      val splitArray = x.trim().split(":")
      (splitArray(0), splitArray(1))
    })

    colonSpliter.toMap // return as Map
  }

  /**
   *
   * @param spark
   * @param seedDF     : customer_id
   * @param universeDF : customer_id
   * @return
   */

  def randomModel(spark: SparkSession, seedDF: DataFrame, universeDF: DataFrame): DataFrame = {

    universeDF.orderBy(rand()).withColumn("score", lit(1.0)).select("customer_id", "score")

  }

  def modelResults(
    session: SparkSession,
    seedDF: DataFrame,
    universeDF: DataFrame,
    affinityDF: Option[DataFrame],
    modelName: String,
    params: Seq[Map[String, String]],
    pooling: String
  ): Seq[DataFrame] = {

    params.map(p => {
      modelResult(session, seedDF, universeDF, affinityDF, modelName, p, pooling)
    })

  }

  /**
   *
   * @param spark
   * @param seedDF
   * @param universeDF
   * @param affinityDF
   * @param modelName must be string via command line arg
   * @param paramMap
   * @return
   */

  def modelResult(
    spark: SparkSession,
    seedDF: DataFrame,
    universeDF: DataFrame,
    affinityDF: Option[DataFrame],
    modelName: String,
    paramMap: Map[String, String],
    pooling: String
  ): DataFrame = {
    // using current model method

    /**
     * input:
     * seedDF(customer_id:Long)
     * universeDF(customer_id:Long)
     * affinityDF(customer_id:Long, top_cats:Seq[String])
     * modelName(String -> used to case )
     *
     * output:
     * DataFrame(customer_id:Long, score:Double)
     *
     */

    import spark.implicits._

    // these values below must be same as AWS lambda code has.

    val result: DataFrame = modelName match {

      case "random" => randomModel(spark, seedDF, universeDF)

      case "exact" => computeLookalikeExact(spark, seedDF, universeDF.select("customer_id"), affinityDF.get,
        paramMap.getOrElse("concat_n", "NONE").toString.toInt).withColumn("score", lit(1.0))
      // because of exact matching, if captured, score would be 1.0

      case "lsh" => computeLookalikeLSH(affinityDF.get, seedDF, universeDF.select("customer_id"),
        paramMap.getOrElse("concat_n", "NONE").toString.toInt,
        paramMap.getOrElse("distanceThreshold", "NONE").toString.toDouble,
        paramMap.getOrElse("kendallThreshold", "NONE").toString.toDouble)(spark)
        .withColumnRenamed("CustomerId", "customer_id")
        .select("customer_id", "score").distinct()

      case "lsh-featured" => computeLookalikeLSHWithFeatures(seedDF, universeDF,
        paramMap.getOrElse("concat_n", "NONE").toString.toInt,
        paramMap.getOrElse("distanceThreshold", "NONE").toString.toDouble,
        paramMap.getOrElse("kendallThreshold", "NONE").toString.toDouble,
        pooling)(spark)
        .withColumnRenamed("CustomerId", "customer_id")
        .select("customer_id", "score").distinct()

      case _ => Seq((0L, 0.0D)).toDF("customer_id", "score")
    }

    result.select("customer_id", "score").distinct() //reformatting for precision recall calculation

  }

  private def loadFeatureVector(
    vectorMap: Map[String, UserVector],
    name: String,
    spark: SparkSession,
    targetDateStr: String,
    settings: Settings
  ): Option[DataFrame] = {

    if (vectorMap.contains(name)) {
      Some(vectorMap(name).EvalVectorGeneration(spark, targetDateStr, settings))
    } else {
      None
    }
  }

}
