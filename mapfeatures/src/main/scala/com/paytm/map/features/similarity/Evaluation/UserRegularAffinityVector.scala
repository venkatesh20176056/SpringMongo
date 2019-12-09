package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserRegularAffinityVector extends UserVector {

  override def EvalVectorGeneration(spark: SparkSession, vectorCreationDateStr: String, settings: Settings): DataFrame = {

    val basePath = s"${settings.featuresDfs.shinraRankedCategories}".replace("stg", "prod")
    loadCategoryScore(spark, vectorCreationDateStr, settings, basePath)

  }

}
