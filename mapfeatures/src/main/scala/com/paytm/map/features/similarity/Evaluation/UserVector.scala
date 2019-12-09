package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.logger
import com.paytm.map.features.utils.{FileUtils, Settings}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class UserVector {

  def EvalVectorGeneration(spark: SparkSession, vectorCreationDateStr: String, setting: Settings): DataFrame

  def loadCategoryScore(
    spark: SparkSession,
    vectorCreationDateStr: String,
    settings: Settings,
    basePath: String,
    datePrefix: String = ""
  ): DataFrame = {

    val path = FileUtils.getLatestTablePath(basePath, spark, vectorCreationDateStr, datePrefix)
    path match {
      case Some(x) =>
        val tfScore = spark.read.load(x)
        tfScore
      case _ =>
        logger.error(s"Cannot load category score")
        sys.exit(1)
    }
  }

}
