package com.paytm.map.features.similarity.Model

import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

abstract class SimilarUserMethods {

  def computeSeeds(inputDt: Option[DateTime], filePath: Option[String], settings: Settings)(implicit spark: SparkSession): Either[String, DataFrame]

  def computeUniverse(inputDt: DateTime, settings: Settings)(implicit spark: SparkSession): DataFrame

}
