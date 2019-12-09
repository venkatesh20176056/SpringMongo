package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.UDFs.readTableV3
import org.apache.spark.sql.functions._
import com.paytm.map.features.config.Schemas.SchemaRepo.BankSchema

object CopyBankFeatures extends CopyBankFeaturesJob
  with SparkJob with SparkJobBootstrap

trait CopyBankFeaturesJob {
  this: SparkJob =>

  val JobName = "BankFeatures"
  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    import spark.implicits._

    val anchorPath = s"${settings.featuresDfs.baseDFS.anchorPath}/BankV2"
    val aggBankPath = s"${settings.featuresDfs.baseDFS.aggPath}/BankFeatures"

    val bankFeatures: DataFrame =
      spark.read.parquet(settings.bankDfs.bankFeaturesPath)
        .where($"dt".between(startDateStr, targetDateStr))

    // Make the DataFrame
    val salesDataSOI = spark.read.parquet(settings.featuresDfs.baseDFS.salesDataPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .where($"successfulTxnFlag" === 1)

    val debitCardProfile = salesDataSOI
      .where($"product_id" === 146203608)
      .where($"dt".between(startDateStr, targetDateStr))
      .groupBy("customer_id", "dt")
      .agg(
        max(when($"status" === 7, 1).otherwise(0)).as("debit_card"),
        min($"created_at").as("debit_card_request_date"),
        max(when($"status" === 7, $"updated_at").otherwise(null)).as("debit_card_delivered_date")
      ).renameColumns(prefix = "BANK_", excludedColumns = Seq("customer_id", "dt"))

    val bankCards =
      bankCardFeatures(settings, startDateStr, targetDateStr)(spark)
        .renameColumns(prefix = "BANK_", excludedColumns = Seq("customer_id", "dt"))

    val fullBankFeatures = {
      bankFeatures
        .join(debitCardProfile, Seq("customer_id", "dt"), "outer")
        .join(bankCards, Seq("customer_id", "dt"), "outer")
        .alignSchema(BankSchema)
        .write
        .partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    fullBankFeatures.moveHDFSData(dtSeq, aggBankPath)
  }

  def bankCardFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.implicits._

    // Input tables
    val offlineTxnFacts = readTableV3(spark, datalakeDfs.offlineTxnFacts)
    val pgEntityInfo = readTableV3(spark, datalakeDfs.entityInfo)
    val entityPrefs = readTableV3(spark, datalakeDfs.entityPreferences)
    val pgDemographics = readTableV3(spark, datalakeDfs.entityDemographics)
    val alipg = DataTables.getPGAlipayOLAP(spark, datalakeDfs.alipayPgOlap, targetDateStr, startDateStr)
    val pgTxnInfo = DataTables.getPgTxnInfo(spark, settings, targetDateStr, startDateStr)

    val cardTypes = Seq("CC", "DC", "CREDIT_CARD", "DEBIT_CARD")
    val nullOrEmpty = Seq("NULL", "null", "")
    val paymentModes = cardTypes ++ nullOrEmpty :+ "NA"

    val merchantIdExclusionList = {
      val demographicsExclusionList = pgDemographics
        .where(lower($"category").isin("onpaytm", "test", "non transacting"))
        .join(pgEntityInfo, $"entity_id" === $"id")
        .select(
          $"entity_id"
        )

      val entityIdExclusionList = demographicsExclusionList
        .union(
          entityPrefs
            .where($"PREF_TYPE" === 34577774427L and $"PREF_VALUE" === 34577774428L and $"status" === 9376503L)
            .select($"entity_id")
        )
        .distinct

      val pgMerchantIdExclusionList = pgEntityInfo
        .join(entityIdExclusionList, $"id" === $"entity_id", "left_anti")
        .where($"status" === 9376503L)
        .select($"mid")
        .distinct()

      val offlineMerchantIdExclusionList = offlineTxnFacts
        .where($"merchant_type".isin("offline_organized", "offline_unorganized"))
        .select(
          $"merchant_id".cast("string") as "mid"
        )
        .distinct()

      offlineMerchantIdExclusionList
        .union(pgMerchantIdExclusionList)
    }

    val filterCondition = $"card_type".isin(cardTypes: _*) and
      trim($"responsecode") === "01" and
      $"dt".between(startDateStr, targetDateStr)

    val txnInfos = Seq(
      $"customer_id",
      $"mid",
      $"card_issuer",
      $"card_type",
      $"card_category",
      $"payment_mode",
      $"responsecode",
      $"dt"
    )

    val bankCardFeatureCols = Seq(
      $"customer_id",
      struct(
        when($"payment_method".isin("CC", "CREDIT_CARD"), "CREDIT_CARD").otherwise("DEBIT_CARD") as "card_type",
        $"card_issuer" as "issuer_bank",
        $"card_category" as "network"
      ) as "bank_card",
      $"dt"
    )

    val paymentMethod = when(($"payment_mode".isin(paymentModes: _*) or $"payment_mode".isNull) and $"card_type".isNotNull and !$"card_type".isin(nullOrEmpty: _*), $"card_type")
      .otherwise($"payment_mode")

    pgTxnInfo.select(txnInfos: _*)
      .union(alipg.where($"customer_id".isNotNull).select(txnInfos: _*))
      .where(filterCondition)
      .join(broadcast(merchantIdExclusionList), Seq("mid"), "left_anti")
      .withColumn("payment_method", paymentMethod)
      .select(bankCardFeatureCols: _*)
      .groupBy($"customer_id", $"dt")
      .agg(collect_set($"bank_card") as "bank_card_list")
  }

}
