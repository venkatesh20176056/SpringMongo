package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.utils.UDFs.readTableV3
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.superuserid.Constants._
import com.paytm.map.featuresplus.superuserid.Utility.{DFFunctions, DateBound, Functions}
import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, length}

object DataUtility {

  class SelectAndFilterRuleClass(
      primaryCols: Seq[Column] = Seq(),
      filterCondition: Seq[Column] = Seq(),
      conditionalCols: Seq[Column] = Seq(),
      selectCols: Seq[Column] = Seq()
  ) {
    val rowsConditions: Column = (DFFunctions.conditionPrimaryCol(primaryCols) ++ filterCondition).reduce(_ && _)
    val colsCondition: Seq[Column] = conditionalCols ++ selectCols
  }

  class TableConditionClass {

    private val soFilterConditions = Seq(col("payment_status") === 2)
    private val soPrimaryCols = Seq(col("customer_id"), col("id"))
    private val soSelectCols = Seq(
      col("id").cast(LongType).as("order_id"),
      col(customerIdCol).cast(LongType).as(customerIdCol),
      col("phone").as(delPhoneCol),
      col("customer_email").as(emailCol)
    )
    val soConfig =
      new SelectAndFilterRuleClass(primaryCols = soPrimaryCols, filterCondition = soFilterConditions, selectCols = soSelectCols)

    private val soaPrimaryCols = Seq(col("order_id"), col("address"), col("pincode"))
    private val soaSelectCols = Seq(
      col("order_id"),
      col("address_type"),
      concat(col("address"), lit(columnConcat), col("address2"), lit(columnConcat), col("pincode")).as(addressCol)
    )
    val soaConfig = new SelectAndFilterRuleClass(primaryCols = soaPrimaryCols, selectCols = soaSelectCols)

    private val bankFilterCondition = Seq(length(col("account_no")) gt 9)
    private val bankPrimaryCols = Seq(col("payer_id"), col("account_no"))
    private val bankSelectCols = Seq(col("payer_id").as(customerIdCol).cast(LongType), col(bankAccCol))
    val wbConfig = new SelectAndFilterRuleClass(primaryCols = bankPrimaryCols, selectCols = bankSelectCols, filterCondition = bankFilterCondition)

    private val profilePrimaryCols: Seq[Column] = Seq(col(customerIdCol))
    private val profileSelectCols = weakFeaturesOrder.map(r => col(r)) :+ col(customerIdCol)
    val profileConfig = new SelectAndFilterRuleClass(primaryCols = profilePrimaryCols, selectCols = profileSelectCols)

    private val includedServices = Array("broadband", "water", "electricity", "toll", "google play", "financial services", "tap to pay card",
      "gas", "challan", "metro", "mobile", "wifi", "datacard", "toll tag", "cable tv", "dth", "fee payment", "municipal payments", "landline")
    private val includedPayType = Array("paytype", "prepaid", "loan", "postpaid", "insurance", "fee payment", "recharge")
    private val frFilterCondition = Seq(
      length(col("recharge_number_1")) gt 3,
      col("service") isin (includedServices: _*),
      col("paytype") isin (includedPayType: _*)
    )
    private val frPrimaryCols = Seq(col("order_id"), col("recharge_number_1"), col("operator"))
    private val frSelectCols = Seq(
      col("order_id"),
      concat(col("recharge_number_1"), lit(columnConcat), col("operator")).as(ruContactCol)
    )
    val frConfig = new SelectAndFilterRuleClass(primaryCols = frPrimaryCols, selectCols = frSelectCols, filterCondition = frFilterCondition)

    private val uideidPrimaryCol = Seq(col("eid"), col("uid"))
    private val uideidSelectCol = Seq(col("eid").as("entity_id"), col("uid").as(customerIdCol))
    val uideidConfig = new SelectAndFilterRuleClass(primaryCols = uideidPrimaryCol, selectCols = uideidSelectCol)

    private val eiPrimaryCols = Seq(col("id"), col("MID"))
    private val eiSelectCol = Seq(col("id").as("entity_id"), col("MID").as("mid"))
    val eiConfig = new SelectAndFilterRuleClass(primaryCols = eiPrimaryCols, selectCols = eiSelectCol)

    private val mbFilterCondition = Seq(length(col("account_no")) gt 9)
    private val mbPrimaryCols = Seq(col("mid"), col("account_no"))
    private val mbSelectCols = Seq(col("mid"), col("account_no").as(merchantBankCol))
    val mbConfig = new SelectAndFilterRuleClass(primaryCols = mbPrimaryCols, selectCols = mbSelectCols, filterCondition = mbFilterCondition)

    private val devFilterCondition = Seq(length(col("deviceId")) gt 13)
    private val devPrimaryCols = Seq(col("customerId"), col("deviceId"))
    private val deviceSelectCols = Seq(col("customerId").as(customerIdCol), col("deviceId").as(deviceCol))
    val devConfig = new SelectAndFilterRuleClass(primaryCols = devPrimaryCols, selectCols = deviceSelectCols, filterCondition = devFilterCondition)

  }

  val TableCondition = new TableConditionClass

  import TableCondition._

  def readSO(range: DateBound)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.salesOrder, range.endDate, range.startDate).
      filter(soConfig.rowsConditions).select(soConfig.colsCondition: _*)
  }

  def readSOA(range: DateBound)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.salesOrderAdd, range.endDate, range.startDate).
      filter(soaConfig.rowsConditions).select(soaConfig.colsCondition: _*)
  }

  def readWalletBank(range: DateBound)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.walletBankTxn, range.endDate, range.startDate).
      filter(wbConfig.rowsConditions).select(wbConfig.colsCondition: _*)
  }

  def readFR(range: DateBound)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.fulfillementRecharge, range.endDate, range.startDate).
      filter(frConfig.rowsConditions).select(frConfig.colsCondition: _*)
  }

  def readUIDEID(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.uidEidMapper).
      filter(uideidConfig.rowsConditions).select(uideidConfig.colsCondition: _*)
  }

  def readEI(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.entityInfo).
      filter(eiConfig.rowsConditions).select(eiConfig.colsCondition: _*)
  }

  def readMB(range: DateBound)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    readTableV3(spark, settings.datalakeDfs.merchantBank, range.endDate, range.startDate).
      filter(mbConfig.rowsConditions).select(mbConfig.colsCondition: _*)
  }

  def readDeviceData(range: DateBound, Path: PathsClass)(implicit spark: SparkSession, settings: Settings): DataFrame = {
    val deviceBasePath = Path.productionPath(settings.featuresDfs.baseDFS.deviceSignalPath)

    val lookBack = Functions.dateDifference(range.startDate, range.endDate)
    val device = (0 to lookBack).map { index =>
      val indexDate = ArgsUtils.formatter.parseDateTime(range.startDate).plusDays(index).toString("yyyy-MM-dd")
      val out = if (Functions.checkFileExists(s"$deviceBasePath/dt=$indexDate/")) {
        spark.read.parquet(s"$deviceBasePath/dt=$indexDate/").
          filter(devConfig.rowsConditions).
          select(devConfig.colsCondition: _*).
          distinct
      } else {
        null
      }
      out
    }.filter(df => df != null).reduce(_ union _)
    device
  }

  def readProfile(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(path).
      filter(profileConfig.rowsConditions).
      select(profileConfig.colsCondition: _*)
  }
}
