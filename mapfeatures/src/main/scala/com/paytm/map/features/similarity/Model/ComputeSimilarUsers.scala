package com.paytm.map.features.similarity.Model

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.{getOptionArgs, logger, num_partitions}
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{desc, max}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ComputeSimilarUsers extends SparkJob with SparkJobBootstrap {
  this: SparkJob =>

  val JobName = "ComputeSimilarUsers"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(sparkSession: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    // Get the command line parameters

    logger.info(s"Begin - $JobName")

    val startTime = System.nanoTime()
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val optionArgs = getOptionArgs(Map(), args.toList)
    val maxUserLimit = optionArgs('user_limit).toInt
    val segmentId = optionArgs('segment_id)
    val executionTimestamp = optionArgs('execution_timestamp)
    val distanceThreshold = optionArgs('distance_threshold).toDouble
    val kendallThreshold = optionArgs('kendall_threshold).toDouble

    // Interim data paths
    val executionOutputPath = s"${settings.featuresDfs.lookAlike}/$segmentId/$executionTimestamp"

    val similarUsersScoredPath = s"$executionOutputPath/${segmentId}_similar_users_scored"

    val seedPath = s"$executionOutputPath/seed"

    val universePath = s"$executionOutputPath/universe"

    val inputDataDate = ArgsUtils.formatter.parseDateTime(targetDateStr)

    logger.info(s"InputDataDate - $inputDataDate")

    logger.info(s"Reading seed data - $seedPath")

    val seedDF = spark.read.parquet(seedPath)

    logger.info(s"Reading universe data - $universePath")

    val universeDF = spark.read.parquet(universePath).join(seedDF, Seq("customer_id"), "leftanti").distinct()

    val affinityBasePath = s"${settings.featuresDfs.lookAlike}/features/user_hashes/"

    val modelOutputBasePath = s"${settings.featuresDfs.lookAlike}/features/model/"

    val dateStr = getLatestTableDate(affinityBasePath, spark, targetDateStr)

    dateStr match {

      case Some(x) => {

        val affinityPath = s"$affinityBasePath/$x"

        val modelOutputPath = s"$modelOutputBasePath/$x"

        val affinityDF = spark.read.load(affinityPath)

        val seedAffinityDF = affinityDF.join(seedDF, Seq("customer_id"))

        val universeAffinityDF = affinityDF.join(universeDF, Seq("customer_id"))

        logger.info("Using LSH flow")

        val lshModel = new LSHModel()

        val approxDF = lshModel.computeSimilarUsers(modelOutputPath, seedAffinityDF,
          universeAffinityDF, distanceThreshold)

        val scoredApproxDF = lshModel.computeKendallScore(approxDF, kendallThreshold)

        val lookalikeDF = universeAffinityDF
          .join(scoredApproxDF, Seq("features_n_str"))
          .select($"customer_id".as("CustomerId"), $"KendallTau", $"recency", $"txsize")
          .groupBy($"CustomerId", $"recency", $"txsize")
          .agg(max($"KendallTau").as("score"))
          .where($"CustomerId".gt(0)) // check valid customer id
          .orderBy(desc("score"), desc("recency"), desc("txsize"))
        // generate same result by sorting score then recency then total tx size

        logger.info(s"Saving the neighbors at $similarUsersScoredPath")

        logger.info(s"Maximum user limit - $maxUserLimit")

        lookalikeDF.select("CustomerId")
          .coalesce(1)
          .limit(maxUserLimit)
          .write.mode(SaveMode.Overwrite).option("header", "true")
          .csv(similarUsersScoredPath)

        val timeSpent = (System.nanoTime() - startTime) / 1e9d

        logger.info(s"RUNTIME in sec-$timeSpent")

        logger.info(s"End - $JobName")
      }

      case None => {
        logger.error(s"Similar customers not generated")
        sys.exit(1)
      }
    }

  }

}
