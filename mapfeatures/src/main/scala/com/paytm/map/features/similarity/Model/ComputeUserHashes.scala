package com.paytm.map.features.similarity.Model

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.similarity.{getOptionArgs, logger, prepareSparseDF}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ComputeUserHashes extends SparkJob with SparkJobBootstrap {

  this: SparkJob =>

  override val JobName: String = "ComputeUserHashes"

  override def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  override def runJob(sparkSession: SparkSession, settings: Settings, args: Array[String]): Unit = {

    implicit val spark: SparkSession = sparkSession

    logger.info(s"Begin - $JobName")

    val startTime = System.nanoTime()
    val targetDateStr = ArgsUtils.getTargetDateStr(args)

    val optionArgs = getOptionArgs(Map(), args.toList)
    val concatN = optionArgs('concat_n).toInt

    val inputDataDate = ArgsUtils.formatter.parseDateTime(targetDateStr).toString(ArgsUtils.formatter)

    logger.info(s"inputDataDate - $inputDataDate")

    val executionOutputPath = s"${settings.featuresDfs.lookAlike}/features/user_hashes/$inputDataDate"

    val executionModelOutputPath = s"${settings.featuresDfs.lookAlike}/features/model/$inputDataDate"

    val featurePath = s"${settings.featuresDfs.shinraRankedCategories}/$inputDataDate"
      .replace("stg", "prod")

    val featureDF = spark.read.load(featurePath)
      .withColumnRenamed("top_cats", "features")

    isValidSchema(featureDF) match {
      case Right(df) =>
        val sparseDF = prepareSparseDF(df, concatN)
        val modelInfo = new LSHModel().buildModel(sparseDF)
        val transformedDF = modelInfo.df
          .select("customer_id", "features_n", "features_n_str", "features_sparse", "hashes")
        transformedDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(executionOutputPath)
        modelInfo.model.write.overwrite().save(executionModelOutputPath)
      case Left(s) =>
        logger.error(s)
        throw new IllegalArgumentException(s)
    }

    val timeSpent = (System.nanoTime() - startTime) / 1e9d

    logger.info(s"RUNTIME in sec-$timeSpent")

    logger.info(s"End - $JobName")

  }

  def isValidSchema(df: DataFrame): Either[String, DataFrame] = {

    val expectedSchema = Seq(LongType, ArrayType(StringType, containsNull = true))

    val dfSchema = df.schema.map(_.dataType)

    val isValidSchema = expectedSchema.zipAll(dfSchema, "", "").forall { case (a, b) => a == b }

    if (isValidSchema) {
      Right(df.toDF("customer_id", "features"))
    } else {
      Left("Invalid schema")
    }

  }

}
