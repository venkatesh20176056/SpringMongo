package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings, UDFs}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.{SaveMode, SparkSession}

object MerchantSuperCashback extends MerchantSuperCashbackJob with SparkJob with SparkJobBootstrap

trait MerchantSuperCashbackJob {
  this: SparkJob =>

  val JobName = "MerchantSuperCashback"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args)
    val baseDFSPath = settings.featuresDfs.baseDFS
    val outPath = s"${baseDFSPath.aggPath}/MerchantSuperCashback/"

    UDFs.readTableV3(spark, settings.datalakeDfs.merchantSupercash)
      .createOrReplaceTempView("merchant_promo_supercash")

    val cashbackStatus = spark.sql(
      """
        |select distinct
        | merchant_id,
        | date(last_campaign_date) as dt,
        | struct(campaign_id,
        |        case when last_updated_status = 2 then 'Active'
        |             when last_updated_status = 3 then 'Redeemed'
        |             when last_updated_status = 4 then 'Expired'
        |             else 'NA' end as last_updated_status,
        |        successful_transaction_count
        |       ) as SCB_supercash_status
        | from (
        |   select
        |    merchant_id,
        |    campaign_id,
        |    first(status) OVER (PARTITION BY merchant_id, campaign_id ORDER BY created_at, updated_at DESC) as last_updated_status,
        |    first(created_at) OVER (PARTITION BY merchant_id, campaign_id ORDER BY created_at, updated_at DESC) as last_campaign_date,
        |    first(txn_count) OVER (PARTITION BY merchant_id, campaign_id ORDER BY created_at, updated_at DESC) as successful_transaction_count
        |  from merchant_promo_supercash
        |)
      """.stripMargin
    )

    cashbackStatus
      .repartition(20, $"dt")
      .write
      .partitionBy("dt")
      .mode(SaveMode.Overwrite)
      .parquet(outPath)
  }

}
