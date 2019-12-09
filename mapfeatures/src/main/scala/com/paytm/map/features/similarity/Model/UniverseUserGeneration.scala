package com.paytm.map.features.similarity.Model

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.{getOptionArgs, logger, tagMap}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import com.paytm.map.features.similarity.baseDataPrep
import org.apache.spark.sql.functions.max

object UniverseUserGeneration extends SparkJob with SparkJobBootstrap {
  this: SparkJob =>

  val JobName = "UniverseUserGeneration"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(sparkSession: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import com.paytm.map.features.utils.ConvenientFrame._

    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    logger.info(s"Begin - $JobName")

    val startTime = System.nanoTime()
    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val optionArgs = getOptionArgs(Map(), args.toList)
    val segmentId = optionArgs('segment_id)
    val executionTimestamp = optionArgs('execution_timestamp)
    val tag = optionArgs('tag)

    val executionOutputPath = s"${settings.featuresDfs.lookAlike}/$segmentId/$executionTimestamp"

    logger.info(s"TargetDateStr - $targetDateStr")

    val inputDataDate = ArgsUtils.formatter.parseDateTime(targetDateStr)

    logger.info(s"InputDataDate - $inputDataDate")

    logger.info(s"Using custom category - $tag")

    logger.info(s"Using custom $tag for universe generation")
    val obj = tagMap.getOrElse(tag, tagMap("default"))

    val recencySizeColumns = Seq("customer_id", "last_seen_paytm_app_date", "PAYTM_total_transaction_size")

    val recencyDF = baseDataPrep(recencySizeColumns, inputDataDate, settings)
      .select($"customer_id", $"last_seen_paytm_app_date".as("recency_date"), $"PAYTM_total_transaction_size".as("tx_size"))
      .groupBy("customer_id")
      .agg(max("recency_date").as("recency"), max("tx_size").as("txsize")) // check duplication with different recency_date

    val universeDF = obj.computeUniverse(inputDataDate, settings).join(recencyDF, Seq("customer_id"))

    logger.info(s"Saving the universe at $executionOutputPath/universe")

    universeDF.coalesce(2).saveParquet(s"$executionOutputPath/universe", withRepartition = false)

    val timeSpent = (System.nanoTime() - startTime) / 1e9d

    logger.info(s"RUNTIME in sec-$timeSpent")

    logger.info(s"End - $JobName")

  }
}
