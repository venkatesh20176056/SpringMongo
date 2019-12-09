package com.paytm.map.features.similarity.Model

import org.apache.spark.sql.DataFrame

abstract class SimilarUserModels {

  type T

  case class ModelInfo(model: T, df: DataFrame)

  def buildModel(df: DataFrame): ModelInfo = {
    val m = fitModel(df)
    val result_df = transformModel(m, df)
    ModelInfo(m, result_df)
  }

  protected def fitModel(df: DataFrame): T

  protected def transformModel(model: T, df: DataFrame): DataFrame

  protected def computeNeighbors(model: T, dfA: DataFrame, dfB: DataFrame, threshold: Double): DataFrame

}
