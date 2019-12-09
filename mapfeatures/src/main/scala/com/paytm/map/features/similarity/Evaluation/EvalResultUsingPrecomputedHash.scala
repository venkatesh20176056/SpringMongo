package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.Model.LSHModel
import com.paytm.map.features.similarity.{getOptionArgs, logger}
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{desc, lit, mean, max, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EvalResultUsingPrecomputedHash extends EvalResultUsingPrecomputedHashJob
  with SparkJob with SparkJobBootstrap

trait EvalResultUsingPrecomputedHashJob {
  this: SparkJob =>

  val JobName = "EvalResultUsingPrecomputedHash"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    val optionArgs = getOptionArgs(Map(), args.toList)
    val segmentId = optionArgs('segment_id)
    val execTime = optionArgs('execution_timestamp)
    val basePath = s"${settings.featuresDfs.lookAlike}/evaluation/metrics/precision_recall/$segmentId/$execTime"
    val oldModelResultPath = s"$basePath/old"
    val newModelResultPath = s"$basePath/new"
    val groundTruthUniversePath = s"$basePath/truth"

    val seedPath = optionArgs('seed_path)
    val universePath = optionArgs('universe_path)
    //val seedPath = s"${settings.featuresDfs.lookAlike}/$segmentId/$execTime/seed"
    //val universePath = s"${settings.featuresDfs.lookAlike}/$segmentId/$execTime/universe"

    val positiveSampleRatio = optionArgs('positive_sample_ratio).toDouble // double such as 0.7

    val oldModelName = optionArgs('old_model_name)
    val newModelName = optionArgs('new_model_name)

    val oldModelParam = optionArgs('old_model_param)
    val newModelParam = optionArgs('new_model_param)

    val seedUniverseType = optionArgs('eval_seed_universe_type) // such as offline
    val removeCustomersOnyHavingClicks = optionArgs('rm_customers_only_clicks)
    val parameterCombinationSize = optionArgs('param_combination_size).toInt
    val maxUserLimit = optionArgs('user_limit).toInt

    val modelOutputBasePath = s"${settings.featuresDfs.lookAlike}/features/model/"
    val affinityBasePath = s"${settings.featuresDfs.lookAlike}/features/user_hashes/"
    val affinityScoreDate = ArgsUtils.formatter.parseDateTime(targetDateStr)

    val seedUniverseMap: Map[String, EvalSeedAndUniverse] = Map(
      "offline" -> EvalSeedAndUniverseOffline,
      "regular" -> EvalSeedAndUniverseRegular,
      "relative_recall" -> EvalSeedAndUniverseRelativeRecall
    )

    val customersOnlyHavingClicks = removeCustomersOnyHavingClicks match {
      case "yes" => Some(UserTrendFilteringAffinityVector.getCustomersOnlyHavingClicks(spark, targetDate, settings))
      case _     => None
    }

    val seedUniverseMapResult = seedUniverseMap.getOrElse(seedUniverseType, EvalSeedAndUniverseOffline)
      .generate(spark, affinityScoreDate, settings, seedPath, universePath, groundTruthUniversePath, positiveSampleRatio, customersOnlyHavingClicks)

    // parameter provided by command args
    val oldParams: Seq[Map[String, String]] = parseParam(oldModelParam)
    val newParams: Seq[Map[String, String]] = parseParam(newModelParam)

    val dateStr = getLatestTableDate(affinityBasePath, spark, targetDateStr)
    dateStr match {
      case Some(x) => {
        val affinityPath = s"$affinityBasePath/$x"
        val modelOutputPath = s"$modelOutputBasePath/$x"
        val affinityDF = spark.read.load(affinityPath).distinct()

        val oldResults: Seq[DataFrame] = modelResults(spark, seedUniverseMapResult, affinityDF, oldModelName, modelOutputPath, maxUserLimit, oldParams, basePath)
        val newResults: Seq[DataFrame] = modelResults(spark, seedUniverseMapResult, affinityDF, newModelName, modelOutputPath, maxUserLimit, newParams, basePath)

        assert(parameterCombinationSize == oldResults.size, "parameterCombinationSize != oldResults.size") // should same
        assert(parameterCombinationSize == newResults.size, "parameterCombinationSize != newResults.size") // should same

        for ((index, df) <- (1 to parameterCombinationSize) zip oldResults) {
          df.coalesce(1).write.mode("overwrite").parquet(s"$oldModelResultPath/$index")
        }

        for ((index, df) <- (1 to parameterCombinationSize) zip newResults) {
          df.coalesce(1).write.mode("overwrite").parquet(s"$newModelResultPath/$index")
        }
      }
      case None => {
        logger.error(s"Similar customers not generated")
        sys.exit(1)
      }
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

  def modelResults(
    session: SparkSession,
    seedUniverseMapResult: Map[String, DataFrame],
    affinityDF: DataFrame,
    modelName: String,
    modelOutputPath: String,
    maxUserLimit: Int,
    params: Seq[Map[String, String]],
    basePath: String
  ): Seq[DataFrame] = {

    params.map(p => {
      modelResult(session, seedUniverseMapResult, affinityDF, modelName, modelOutputPath, maxUserLimit, p, basePath)
    })

  }

  def randomModel(spark: SparkSession, universeDF: DataFrame): DataFrame = {
    universeDF.orderBy(rand()).withColumn("score", lit(1.0))
  }

  def modelResult(
    spark: SparkSession,
    seedUniverseMapResult: Map[String, DataFrame],
    affinityDF: DataFrame,
    modelName: String,
    modelOutputPath: String,
    maxUserLimit: Int,
    paramMap: Map[String, String],
    basePath: String
  ): DataFrame = {

    import spark.implicits._

    // these values below must be same as AWS lambda code has.

    val result: DataFrame = modelName match {

      case "random" =>
        val universeDF: DataFrame = seedUniverseMapResult("universe").distinct()
        randomModel(spark, universeDF)

      case "random_affinity" =>
        val universeDF: DataFrame = seedUniverseMapResult("universe").distinct()
        val universeAffinityDF = affinityDF.join(universeDF, Seq("customer_id"))
        randomModel(spark, universeAffinityDF)

      case "lsh" =>

        // full seed run
        if (seedUniverseMapResult.contains("seedAll") && seedUniverseMapResult.contains("uninverseAll")) {
          val fseedDF = seedUniverseMapResult("seedAll").distinct()
          val funiverseDF = seedUniverseMapResult("uninverseAll").distinct()
          val flookalikeDF = lsh(spark, fseedDF, funiverseDF, affinityDF, modelName, modelOutputPath, maxUserLimit, paramMap)
          flookalikeDF.coalesce(1).write.mode("overwrite").parquet(s"$basePath/full_seed_lsh")
        }

        // positive ratio run
        val seedDF: DataFrame = seedUniverseMapResult("seed").distinct()
        val universeDF: DataFrame = seedUniverseMapResult("universe").distinct()
        val lookalikeDF: DataFrame = lsh(spark, seedDF, universeDF, affinityDF, modelName, modelOutputPath, maxUserLimit, paramMap).select("customer_id", "score").coalesce(1)

        val remnant: DataFrame = universeDF.select("customer_id")
          .join(lookalikeDF, Seq("customer_id"), "left_anti")
          .withColumn("score", lit(-1.0))
          .orderBy(rand()).coalesce(1)
        //remnant.coalesce(1).write.mode("overwrite").parquet(s"$basePath/remnant")

        lookalikeDF.union(remnant)

      case _ => Seq((0L, 0.0D)).toDF("customer_id", "score")
    }

    result.select("customer_id", "score") //reformatting for precision recall calculation

  }

  private def lsh(
    spark: SparkSession,
    seedDF: DataFrame,
    universeDF: DataFrame,
    affinityDF: DataFrame,
    modelName: String,
    modelOutputPath: String,
    maxUserLimit: Int,
    paramMap: Map[String, String]
  ): DataFrame = {

    import spark.implicits._

    logger.info("Using LSH flow")

    val seedAffinityDF = affinityDF.join(seedDF, Seq("customer_id"))
    val universeAffinityDF = affinityDF.join(universeDF, Seq("customer_id"))

    val lshModel = new LSHModel()
    val approxDF: DataFrame = lshModel.computeSimilarUsers(modelOutputPath, seedAffinityDF,
      universeAffinityDF, paramMap.getOrElse("distanceThreshold", "0.5").toDouble)

    val scoredApproxDF: DataFrame = lshModel.computeKendallScore(approxDF, paramMap.getOrElse("kendallThreshold", "0.1").toDouble)

    val lookalikeDF: DataFrame = universeAffinityDF
      .join(scoredApproxDF, Seq("features_n_str"))
      .select($"customer_id", $"KendallTau", $"recency", $"txsize")
      .groupBy($"customer_id", $"recency", $"txsize")
      .agg(max($"KendallTau").as("score"))
      .where($"customer_id".gt(0)) // check valid customer id
      .orderBy(desc("score"), desc("recency"), desc("txsize"))
      .limit(maxUserLimit)

    lookalikeDF

  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }
}