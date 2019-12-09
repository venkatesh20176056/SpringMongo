package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.baseDataPrep
import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

/**
 * will be modified for different use case, for example offline vertical case, this one is used in EvalSeedUniverseOffline
 */

abstract class EvalSeedAndUniverse {

  def generate(
    spark: SparkSession,
    date: DateTime,
    settings: Settings,
    trainingData: String,
    baseData: String, /// whole population from which we are looking for interested customers
    testingData: String, /// manually generated testing data, it works as ground truth
    positiveSampleRatio: Double,
    excludedSegment: Option[DataFrame] = None
  ): Map[String, DataFrame]

  protected def dfOrError(mapObj: Map[String, DataFrame], getString: String): DataFrame = {

    mapObj.get(getString) match {
      case Some(x) => x
      case _       => throw new IllegalArgumentException("check input argument at seedUniverseMapResult")
    }

  }

  protected def loadBase(spark: SparkSession, todayDate: DateTime, settings: Settings): DataFrame = {
    val colStrSeq = Seq("customer_id", "OFFLINE_total_transaction_count", "OFFLINE_first_transaction_date", "last_seen_paytm_app_date", "PAYTM_total_transaction_size")
    baseDataPrep(colStrSeq, todayDate, settings)(spark)
  }
}

