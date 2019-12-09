package com.paytm.map.features.similarity.Model

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.{getOptionArgs, logger, tagMap}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.SparkSession

object SeedUserGeneration extends SparkJob with SparkJobBootstrap {
  this: SparkJob =>

  val JobName = "SeedUserGeneration"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(sparkSession: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame

    implicit val spark: SparkSession = sparkSession

    logger.info(s"Begin - $JobName")

    val startTime = System.nanoTime()
    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val optionArgs = getOptionArgs(Map(), args.toList)

    logger.info(optionArgs.map { case (k, v) => k + "->" + v }.mkString("|"))
    val segmentId = optionArgs('segment_id)
    val executionTimestamp = optionArgs('execution_timestamp)
    val userBasePath = optionArgs('user_base_path)
    val tag = optionArgs('tag)

    val executionOutputPath = s"${settings.featuresDfs.lookAlike}/$segmentId/$executionTimestamp"

    val inputDataDate = ArgsUtils.formatter.parseDateTime(targetDateStr)

    logger.info(s"TargetDateStr - $targetDateStr")

    logger.info(s"InputDataDate - $inputDataDate")

    logger.info(s"Using custom tag - $tag")

    val obj = tagMap.getOrElse(tag, tagMap("default"))

    val seedDF = obj.computeSeeds(Some(inputDataDate), Some(userBasePath), settings)

    logger.info(s"Saving the seed at $executionOutputPath/seed")

    seedDF match {
      case Right(x) => x.coalesce(2).saveParquet(s"$executionOutputPath/seed", withRepartition = false)
      case Left(x) =>
        logger.error(s"$x- seed not generated")
        sys.exit(1)

    }

    val timeSpent = (System.nanoTime() - startTime) / 1e9d

    logger.info(s"RUNTIME in sec-$timeSpent")
    logger.info(s"End - $JobName")

  }
}

