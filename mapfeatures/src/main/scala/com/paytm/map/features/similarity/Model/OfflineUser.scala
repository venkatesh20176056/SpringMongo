package com.paytm.map.features.similarity.Model

import com.paytm.map.features.similarity.{baseDataPrep, logger}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object OfflineUser extends SimilarUserMethods {

  private def baseDataOffline(todayDate: DateTime, settings: Settings)(implicit spark: SparkSession): DataFrame = {

    val colStrSeq = Seq(
      "customer_id",
      "OFFLINE_total_transaction_count_180_days",
      "OFFLINE_first_transaction_date",
      "campaigns_opened_6_months",
      "PAYTM_total_transaction_count_180_days",
      "PAYTM_total_product_clicks_app",
      "last_seen_paytm_app_date",
      "OFFLINE_total_transaction_count"
    )

    baseDataPrep(colStrSeq, todayDate, settings)
  }

  override def computeUniverse(todayDate: DateTime, settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    logger.info("Computing universe for offline tag")

    // instead of past 180 days, check offline tx = 0 ever
    val universe = baseDataOffline(todayDate, settings)
      .where($"OFFLINE_total_transaction_count".equalTo(0))
      .select("customer_id")
    universe
  }

  override def computeSeeds(date: Option[DateTime], filePath: Option[String], settings: Settings)(implicit spark: SparkSession): Either[String, DataFrame] = {

    def preComputedSeedOffline(baseDataDF: DataFrame, todayDateStr: String): DataFrame = {

      import spark.implicits._

      val pcSeedPre = baseDataDF
        .select(
          "customer_id",
          "OFFLINE_total_transaction_count_180_days",
          "campaigns_opened_6_months",
          "PAYTM_total_transaction_count_180_days",
          "PAYTM_total_product_clicks_app"
        )

      val pcSeed = pcSeedPre
        .where(
          $"OFFLINE_total_transaction_count_180_days" > 1 &&
            $"campaigns_opened_6_months" > 5 &&
            $"PAYTM_total_transaction_count_180_days" > 5 &&
            $"PAYTM_total_product_clicks_app" > 5
        ) // this condition is provided by Pengcheng Jia

      pcSeed //return

    }

    def newlyConvertSeedOffline(baseDataDF: DataFrame, dateStr: String): DataFrame = {

      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._
      import spark.implicits._

      val newlyConvert = baseDataDF
        .withColumn("today", lit(dateStr))
        .withColumn("daysAfterFirstOffline", datediff($"today", $"OFFLINE_first_transaction_date"))
        .where($"daysAfterFirstOffline".lt(90))
        .select($"customer_id".cast(LongType) as "customer_id") //inner join to meet both conditions

      // measure days between each customer's first Offline TX date and today
      // customers who have days < 90 => this means that those customers became offline customer during last 90 days
      //return
      newlyConvert

    }

    logger.info("Computing newly converted seeds for offline tag")

    date match {
      case Some(x) => {
        val baseData = baseDataOffline(x, settings)
        Right(newlyConvertSeedOffline(baseData, x.toString(ArgsUtils.formatter)))
      }
      case None =>
        Left("Param not specified")
    }
  }
}
