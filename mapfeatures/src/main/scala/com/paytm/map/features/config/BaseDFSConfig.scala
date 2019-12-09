package com.paytm.map.features.config

import com.paytm.map.features.utils.ArgsUtils
import org.joda.time.DateTime

case class BaseDFSConfig(
    aggPath: String,
    anchorPath: String,
    salesDataPath: String,
    salesPromoPath: String,
    salesPromMetaPath: String,
    salesPromRechPath: String,
    strPath: String,
    strMerchantPath: String,
    salesPromoCategoryPath: String,
    merchantsPath: String,
    paymentTxnPath: String,
    gaAggregateBase: String,
    gaAggregateLPPath: String,
    lmsTablePath: String,
    lmsTablePathV2: String,
    auditLogsTablePath: String,
    bankPath: String,
    smsTablePath: String,
    upiTxnPath: String,
    deviceSignalPath: String,
    paymentsBasePath: String
) {
  def gaAggregatePath(targetDate: DateTime): String = {
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    s"$gaAggregateBase/$targetDateStr"
  }

  def gaAggregateLPPath(targetDate: DateTime): String = {
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    s"$gaAggregateLPPath/$targetDateStr"
  }
}

object BaseDFSConfig {
  def apply(basePath: String): BaseDFSConfig = {

    val baseTablesPath = s"$basePath/baseTables"
    val aggPath = s"$basePath/aggregates"

    BaseDFSConfig(
      aggPath                = aggPath,
      anchorPath             = s"$aggPath/cache/",
      salesDataPath          = s"$baseTablesPath/sales",
      salesPromoPath         = s"$baseTablesPath/sales_promo",
      salesPromMetaPath      = s"$baseTablesPath/sales_promo_meta",
      salesPromRechPath      = s"$baseTablesPath/sales_promo_Rech",
      strPath                = s"$baseTablesPath/newstr",
      strMerchantPath        = s"$baseTablesPath/newstr_merchant",
      salesPromoCategoryPath = s"$baseTablesPath/sales_promo_category",
      merchantsPath          = s"$aggPath/merchants",
      paymentTxnPath         = s"$baseTablesPath/payements_data",
      gaAggregateBase        = s"$baseTablesPath/ga_product_rech_cache",
      gaAggregateLPPath      = s"$baseTablesPath/ga_lp_aggregates",
      lmsTablePath           = s"$aggPath/lms",
      lmsTablePathV2         = s"$aggPath/lmsV2",
      auditLogsTablePath     = s"$baseTablesPath/context_data",
      bankPath               = s"$baseTablesPath/bank",
      smsTablePath           = s"$aggPath/SMS",
      upiTxnPath             = s"$baseTablesPath/upi",
      deviceSignalPath       = s"$baseTablesPath/signal",
      paymentsBasePath       = s"$aggPath/payements_data"
    )
  }
}
