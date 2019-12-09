package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo.BankSchema
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs.readTableV3
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

object BankFeatures extends BankFeaturesJob
  with SparkJob with SparkJobBootstrap

trait BankFeaturesJob {
  this: SparkJob =>

  val JobName = "BankFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggBankPath = s"${baseDFS.aggPath}/BankFeatures"
    val anchorPath = s"${baseDFS.anchorPath}/Bank"

    import spark.implicits._

    /** ***************************** Debit Card Features ************************************/
    // Make the DataFrame
    val salesDataSOI = spark.read.parquet(baseDFS.salesDataPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .where($"successfulTxnFlag" === 1)

    val DebitCardProfile = salesDataSOI
      .where($"product_id" === 146203608)
      .groupBy("customer_id", "dt")
      .agg(
        max(when($"status" === 7, 1).otherwise(0)).as("debit_card"),
        min($"created_at").as("debit_card_request_date"),
        max(when($"status" === 7, $"updated_at").otherwise(null)).as("debit_card_delivered_date")
      )

    /** ***************************** Bank Card Features ************************************/
    val bankCards = bankCardFeatures(settings, startDateStr, targetDateStr)(spark)

    /** ******************************** Banking Features **************************************/

    val salesData = spark.read.parquet(baseDFS.bankPath)
      .addBankLevel()
      .addInOutLevel()
      .select(
        $"customer_id",
        $"dt",
        $"InOut_Level",
        $"Bank_Level",
        $"tran_id".as("order_item_id"),
        $"tran_amt".as("selling_price"),
        $"created_at"
      )
      .repartition($"dt", $"customer_id")
      .cache()

    val bankLevels = Seq("IMPS_OUT", "IMPS_IN", "NETBANK_OUT", "POS_OUT", "NEFT_OUT", "NEFT_IN", "RTGS_OUT", "RTGS_IN", "UPI_OUT", "UPI_IN", "ATM_OUT", "ATM_IN", "BC_OUT", "BC_IN", "BRCH_OUT", "BRCH_IN", "ECOM_OUT", "B2B_IN")
    val inOutLevels = Seq("Inward", "Outward")

    // Generate Sales Aggregates
    val salesAggregates = salesData
      .addSalesAggregates(Seq("customer_id", "dt"), "Bank_Level", pivotValues = bankLevels, isDiscount = false)

    // Generate Sales First and Last Aggregate
    val firstLastTxn = salesData
      .addFirstLastCol(Seq("customer_id", "dt", "Bank_Level"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "Bank_Level", pivotValues = bankLevels)

    /** **************************** Banking Overall Features **********************************/
    // Generate Sales Aggregates
    val salesAggregatesOverAll = salesData
      .addSalesAggregates(Seq("dt", "customer_id"), pivotCol = "InOut_Level", pivotValues = inOutLevels, isDiscount = false)

    // Generate Sales First and Last Aggregate
    val firstLastTxnOverAll = salesData
      .addFirstLastCol(Seq("customer_id", "dt", "InOut_Level"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "InOut_Level", pivotValues = inOutLevels)

    // Final Aggregates
    val dataDF = {
      salesAggregatesOverAll
        .join(firstLastTxnOverAll, Seq("customer_id", "dt"), "left_outer")
        .join(salesAggregates, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(DebitCardProfile, Seq("customer_id", "dt"), "outer")
        .join(bankCards, Seq("customer_id", "dt"), "outer")
        .renameColumns(prefix = "BANK_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(BankSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggBankPath)
  }

  private implicit class BankImplicits(dF: DataFrame) {

    def addBankLevel(): DataFrame = {
      import dF.sparkSession.implicits._
      dF.withColumn(
        "Bank_Level",
        when($"rpt_code".isin(20212, 20271), "IMPS_OUT")
          .when($"rpt_code".isin(20211, 20270), "IMPS_IN")
          .when($"rpt_code".isin(60202, 80203, 80204), "NETBANK_OUT")
          .when($"rpt_code".isin(70203, 70204), "POS_OUT")
          .when($"rpt_code" === 20420, "NEFT_OUT")
          .when($"rpt_code" === 20410, "NEFT_IN")
          .when($"rpt_code" === 20440, "RTGS_OUT")
          .when($"rpt_code" === 20430, "RTGS_IN")
          .when($"rpt_code" === 20502, "UPI_OUT")
          .when($"rpt_code" === 20501, "UPI_IN")
          .when($"rpt_code" === 70201, "ATM_OUT")
          .when($"rpt_code" === 70233, "ATM_IN")
          .when($"rpt_code" === 60201, "BC_OUT")
          .when($"rpt_code" === 60200, "BC_IN")
          .when($"rpt_code" === 20110, "BRCH_OUT")
          .when($"rpt_code" === 20100, "BRCH_IN")
          .when($"rpt_code" === 20210, "ECOM_OUT")
          .when($"rpt_code" === 90100, "B2B_IN")
          .otherwise(null)
      ).where($"Bank_Level".isNotNull)
    }

    def addInOutLevel(): DataFrame = {
      import dF.sparkSession.implicits._
      dF.withColumn(
        "InOut_Level",
        when($"Bank_Level".like("%_IN"), "Inward")
          .when($"Bank_Level".like("%_OUT"), "Outward")
          .otherwise(null)
      ).where($"InOut_Level".isNotNull)
    }
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