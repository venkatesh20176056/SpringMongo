package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.baseDataPrep
import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object UserTrendFilteringAffinityVector extends UserVector {

  override def EvalVectorGeneration(spark: SparkSession, vectorCreationDateStr: String, settings: Settings): DataFrame = {

    val basePath = s"${settings.featuresDfs.weightedTrendFilteringTopCategories}".replace("stg", "prod")
    loadCategoryScore(spark, vectorCreationDateStr, settings, basePath)
  }

  def getCustomersOnlyHavingClicks(spark: SparkSession, date: DateTime, settings: Settings): DataFrame = {

    //val basePath = s"${settings.featuresDfs.weightedTrendFilteringCategories}".replace("stg", "prod")
    import spark.implicits._
    val colStrSeq = Seq(
      "customer_id",
      "PAYTM_last_transaction_date",
      "PAYTM_total_transaction_count"
    )

    val custms = baseDataPrep(colStrSeq, date, settings)(spark)
      .where($"PAYTM_total_transaction_count".equalTo(0))
      .select("customer_id")

    custms
  }

}
