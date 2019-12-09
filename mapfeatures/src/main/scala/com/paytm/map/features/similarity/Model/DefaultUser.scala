package com.paytm.map.features.similarity.Model

import com.paytm.map.features.similarity.logger
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object DefaultUser extends SimilarUserMethods {

  override def computeSeeds(date: Option[DateTime], filePath: Option[String], settings: Settings)(implicit spark: SparkSession): Either[String, DataFrame] = {
    def loadBaseUser(userBasePath: String)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      spark.read.option("header", "false").csv(userBasePath)
        .withColumnRenamed("_c0", "customer_id")
        .filter($"customer_id".isNotNull && $"customer_id".cast(LongType).isNotNull)
        .select($"customer_id".cast(LongType) as "customer_id")
    }

    logger.info("Using default implementation for seed computation")

    filePath match {
      case Some(x) =>
        Right(loadBaseUser(x))
      case None =>
        Left("Param not specified")
    }
  }

  override def computeUniverse(date: DateTime, settings: Settings)(implicit spark: SparkSession): DataFrame = {

    logger.info("Using default implementation for universe computation")

    val basePath = s"${settings.featuresDfs.lookAlike}/features/user_hashes/"
    val dateStr = getLatestTableDate(basePath, spark, date.toString(ArgsUtils.formatter))

    dateStr match {
      case Some(x) =>
        val affinityPath = s"$basePath/$x"
        spark.read.load(affinityPath).select("customer_id")
      case _ =>
        logger.error("No recent dateString found for universe data")
        sys.exit(1)
    }
  }
}
