package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.config.Schemas.FewMoreSchema.PaymentsTxnSchema
import com.paytm.map.features.base.DataTables._

import scala.util.Try

object PaymentTxnBaseData extends PaymentTxnBaseDataJob
  with SparkJob with SparkJobBootstrap

trait PaymentTxnBaseDataJob {
  this: SparkJob =>

  val JobName = "PaymentTxnBaseData"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDate = targetDate.minusDays(lookBackDays)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(startDate, targetDate, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val merchantsPath = baseDFS.merchantsPath
    val strPath = baseDFS.strPath
    val paymentTxnPath = baseDFS.paymentTxnPath
    val paymentsBasePath = baseDFS.paymentsBasePath
    val newSTRPath = s"$paymentTxnPath/source=newSTR"
    val alipayPath = s"$paymentTxnPath/source=alipay"

    // Make the DataFrames
    lazy val merchantTable = DataTables.merchantTable(spark, settings.datalakeDfs.merchantUser)
    lazy val offlineOrganizedMerchant = DataTables.offlineOrganizedMerchant(spark, settings.datalakeDfs.offOrgMerchant)
    lazy val eidUidMapper = DataTables.UIDEIDMapper(spark, settings.datalakeDfs.uidEidMapper)
    lazy val offlineRetailer = DataTables.offlineRetailer(spark, settings.datalakeDfs.offRetailer)
    lazy val entityInfo = DataTables.entityInfo(spark, settings.datalakeDfs.entityInfo)
    lazy val entityDemographics = DataTables.entityDemographics(spark, settings.datalakeDfs.entityDemographics)
    lazy val entityPreferences = DataTables.entityPreferences(spark, settings.datalakeDfs.entityPreferences)
    lazy val alipayPGOlap: Either[Throwable, DataFrame] = try {
      Right(DataTables.getPGAlipayOLAP(spark, settings.datalakeDfs.alipayPgOlap, targetDateStr, startDateStr))
    } catch {
      case e: Exception =>
        Left(e)
    }

    lazy val offlineRFSE = DataTables.offlineRFSE(spark, settings.datalakeDfs.offlineRFSE)

    lazy val allMerchants = DataTables.getMerchantData(offlineOrganizedMerchant, eidUidMapper, offlineRetailer, merchantTable, entityInfo, entityDemographics, entityPreferences).cache

    // Generate All Merchants Data
    allMerchants
      .write.mode(SaveMode.Overwrite)
      .parquet(merchantsPath)

    // Generate TXNal data
    val allNewSTR = spark.read.parquet(strPath)
      .where(col("dt").between(startDateStr, targetDateStr))
    val selectCols = Seq("customer_id", "payment_type", "order_item_id", "selling_price", "merchant_L2_type",
      "merchant_L1_type", "merchant_category", "merchant_subcategory", "dt", "created_at", "mid", "alipay_trans_id", "byUPI", "byNB", "is_pg_drop_off", "successfulTxnFlag", "payment_mode", "request_type")

    // New STR Data
    DataTables.getNewSTRPaymentsTable(allNewSTR, allMerchants, offlineRFSE)
      .where(col("customer_id").isNotNull)
      .select(selectCols.head, selectCols.tail: _*)
      .alignSchema(PaymentsTxnSchema)
      .coalescePartitions("dt", "customer_id", dtSeq)
      .write.partitionBy("dt")
      .mode(SaveMode.Overwrite).parquet(newSTRPath)

    // Alipay Data Path
    val newSTRTxn = spark.read.parquet(newSTRPath)
      .select("alipay_trans_id")
      .where(col("alipay_trans_id").isNotNull)

    alipayPGOlap match {
      case Right(df) => {
        DataTables.getAlipayOlapPaymentsTable(df, allMerchants)
          .join(newSTRTxn, Seq("alipay_trans_id"), "left_anti")
          .select(selectCols.head, selectCols.tail: _*)
          .alignSchema(PaymentsTxnSchema)
          .coalescePartitions("dt", "customer_id", dtSeq)
          .write.partitionBy("dt")
          .mode(SaveMode.Overwrite).parquet(alipayPath)
      }
      case Left(e) => log.warn("Can't read alipay olap table: ", e)
    }

    // append the data to the persisted data.
    spark.read.parquet(paymentTxnPath)
      .moveHDFSData(dtSeq, paymentsBasePath)
  }
}
