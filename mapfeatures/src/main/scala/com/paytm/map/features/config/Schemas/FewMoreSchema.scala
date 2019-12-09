package com.paytm.map.features.config.Schemas

import org.apache.spark.sql.types._

object FewMoreSchema {

  val L1Schema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("L1_total_transaction_count", LongType),
    StructField("L1_total_transaction_size", DoubleType),
    StructField("L1_first_transaction_date", StringType),
    StructField("L1_last_transaction_date", StringType),
    StructField("L1_total_discount_transaction_count", LongType),
    StructField("L1_total_discount_transaction_size", DoubleType),
    StructField("L1_first_transaction_size", DoubleType),
    StructField("L1_last_transaction_size", DoubleType),
    StructField("L1_total_cashback_transaction_count", LongType),
    StructField("L1_total_cashback_transaction_size", DoubleType),
    StructField("L1_promocodes_used", ArrayType(StructType(Seq(StructField("Promocode", StringType), StructField("Date", StringType))))),
    StructField("dt", StringType)
  ))

  val L2Schema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("L2_BK_total_transaction_count", LongType),
    StructField("L2_BK_total_transaction_size", DoubleType),
    StructField("L2_BK_first_transaction_date", StringType),
    StructField("L2_BK_last_transaction_date", StringType),
    StructField("L2_BK_total_discount_transaction_count", LongType),
    StructField("L2_BK_total_discount_transaction_size", DoubleType),
    StructField("L2_EC_total_transaction_count", LongType),
    StructField("L2_EC_total_transaction_size", DoubleType),
    StructField("L2_EC_first_transaction_date", StringType),
    StructField("L2_EC_last_transaction_date", StringType),
    StructField("L2_EC_total_discount_transaction_count", LongType),
    StructField("L2_EC_total_discount_transaction_size", DoubleType),
    StructField("L2_RU_total_transaction_count", LongType),
    StructField("L2_RU_total_transaction_size", DoubleType),
    StructField("L2_RU_first_transaction_date", StringType),
    StructField("L2_RU_last_transaction_date", StringType),
    StructField("L2_RU_total_discount_transaction_count", LongType),
    StructField("L2_RU_total_discount_transaction_size", DoubleType),
    StructField("L2_BK_first_transaction_size", DoubleType),
    StructField("L2_BK_last_transaction_size", DoubleType),
    StructField("L2_EC_first_transaction_size", DoubleType),
    StructField("L2_EC_last_transaction_size", DoubleType),
    StructField("L2_RU_first_transaction_size", DoubleType),
    StructField("L2_RU_last_transaction_size", DoubleType),
    StructField("L2_BK_total_cashback_transaction_count", LongType),
    StructField("L2_BK_total_cashback_transaction_size", DoubleType),
    StructField("L2_EC_total_cashback_transaction_count", LongType),
    StructField("L2_EC_total_cashback_transaction_size", DoubleType),
    StructField("L2_RU_total_cashback_transaction_count", LongType),
    StructField("L2_RU_total_cashback_transaction_size", DoubleType),
    StructField("L2_RU_last_attempt_order_date", StringType),
    StructField("dt", StringType)
  ))

  val GABKSchema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("BK_TR_total_app_homepage_sessions_count", LongType),
    StructField("BK_TR_total_web_homepage_sessions_count", LongType),
    StructField("BK_FL_total_app_homepage_sessions_count", LongType),
    StructField("BK_FL_total_app_searchpage_sessions_count", LongType),
    StructField("BK_FL_total_app_reviewpage_sessions_count", LongType),
    StructField("BK_FL_total_app_passengerpage_sessions_count", LongType),
    StructField("BK_FL_total_app_orderpage_sessions_count", LongType),
    StructField("BK_FL_total_web_homepage_sessions_count", LongType),
    StructField("BK_FL_total_web_searchpage_sessions_count", LongType),
    StructField("BK_FL_total_web_reviewpage_sessions_count", LongType),
    StructField("BK_FL_total_web_passengerpage_sessions_count", LongType),
    StructField("BK_FL_total_web_orderpage_sessions_count", LongType),
    StructField("BK_BUS_total_app_homepage_sessions_count", LongType),
    StructField("BK_BUS_total_app_searchpage_sessions_count", LongType),
    StructField("BK_BUS_total_app_seatpage_sessions_count", LongType),
    StructField("BK_BUS_total_app_passengerpage_sessions_count", LongType),
    StructField("BK_BUS_total_app_reviewpage_sessions_count", LongType),
    StructField("BK_BUS_total_app_orderpage_sessions_count", LongType),
    StructField("BK_BUS_total_web_homepage_sessions_count", LongType),
    StructField("BK_BUS_total_web_searchpage_sessions_count", LongType),
    StructField("BK_BUS_total_web_seatpage_sessions_count", LongType),
    StructField("BK_BUS_total_web_passengerpage_sessions_count", LongType),
    StructField("BK_BUS_total_web_reviewpage_sessions_count", LongType),
    StructField("BK_BUS_total_web_orderpage_sessions_count", LongType),
    StructField("dt", StringType)
  ))

  val GAL1Schema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("L1GA_total_product_views_app", LongType),
    StructField("L1GA_total_product_views_web", LongType),
    StructField("L1GA_total_product_clicks_app", LongType),
    StructField("L1GA_total_product_clicks_web", LongType),
    StructField("L1GA_total_pdp_sessions_app", LongType),
    StructField("L1GA_total_pdp_sessions_web", LongType),
    StructField("dt", StringType)
  ))

  val GAL2Schema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("L2GA_BK_total_product_views_app", LongType),
    StructField("L2GA_BK_total_product_views_web", LongType),
    StructField("L2GA_BK_total_product_clicks_app", LongType),
    StructField("L2GA_BK_total_product_clicks_web", LongType),
    StructField("L2GA_BK_total_pdp_sessions_app", LongType),
    StructField("L2GA_BK_total_pdp_sessions_web", LongType),
    StructField("L2GA_EC_total_product_views_app", LongType),
    StructField("L2GA_EC_total_product_views_web", LongType),
    StructField("L2GA_EC_total_product_clicks_app", LongType),
    StructField("L2GA_EC_total_product_clicks_web", LongType),
    StructField("L2GA_EC_total_pdp_sessions_app", LongType),
    StructField("L2GA_EC_total_pdp_sessions_web", LongType),
    StructField("dt", StringType)
  ))

  val GAL2RUSchema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("L2GA_RU_total_app_sessions_count", LongType),
    StructField("L2GA_RU_total_web_sessions_count", LongType),
    StructField("dt", StringType)
  ))

  val GAL3RUSchema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("RU_GAS_total_app_sessions_count", LongType),
    StructField("RU_GAS_total_web_sessions_count", LongType),
    StructField("RU_MB_total_app_sessions_count", LongType),
    StructField("RU_MB_total_web_sessions_count", LongType),
    StructField("RU_DTH_total_app_sessions_count", LongType),
    StructField("RU_DTH_total_web_sessions_count", LongType),
    StructField("RU_BB_total_app_sessions_count", LongType),
    StructField("RU_BB_total_web_sessions_count", LongType),
    StructField("RU_DC_total_app_sessions_count", LongType),
    StructField("RU_DC_total_web_sessions_count", LongType),
    StructField("RU_EC_total_app_sessions_count", LongType),
    StructField("RU_EC_total_web_sessions_count", LongType),
    StructField("RU_WAT_total_app_sessions_count", LongType),
    StructField("RU_WAT_total_web_sessions_count", LongType),
    StructField("RU_LDL_total_app_sessions_count", LongType),
    StructField("RU_LDL_total_web_sessions_count", LongType),
    StructField("RU_INS_total_app_sessions_count", LongType),
    StructField("RU_INS_total_web_sessions_count", LongType),
    StructField("RU_LOAN_total_app_sessions_count", LongType),
    StructField("RU_LOAN_total_web_sessions_count", LongType),
    StructField("RU_MTC_total_app_sessions_count", LongType),
    StructField("RU_MTC_total_web_sessions_count", LongType),
    StructField("RU_DV_total_app_sessions_count", LongType),
    StructField("RU_DV_total_web_sessions_count", LongType),
    StructField("RU_GP_total_app_sessions_count", LongType),
    StructField("RU_GP_total_web_sessions_count", LongType),
    StructField("RU_CHL_total_app_sessions_count", LongType),
    StructField("RU_CHL_total_web_sessions_count", LongType),
    StructField("RU_MNC_total_app_sessions_count", LongType),
    StructField("RU_MNC_total_web_sessions_count", LongType),
    StructField("RU_TOLL_total_app_sessions_count", LongType),
    StructField("RU_TOLL_total_web_sessions_count", LongType),
    StructField("dt", StringType)
  ))

  val LMSV2Schema = StructType(Seq(
    StructField("customer_id", StringType),
    StructField("account_details", StructType(
      Seq(
        StructField("AccountNumber", StringType),
        StructField("TotalAmount", DoubleType),
        StructField("BalanceAmount", DoubleType),
        StructField("DueDate", TimestampType),
        StructField("LateFee", DoubleType),
        StructField("ProductSource", StringType),
        StructField("ProductType", StringType)
      )
    )),
    StructField("created_at", TimestampType),
    StructField("updated_at", TimestampType),
    //    StructField("id", DoubleType),
    StructField("account_number", StringType),
    StructField("due_date", TimestampType),
    StructField("total_amount", DoubleType),
    StructField("balance_amount", DoubleType),
    StructField("late_fee", DoubleType),
    StructField("product_source", StringType),
    StructField("product_type", IntegerType),
    //    StructField("status", IntegerType),
    StructField("contact_by_SMS", IntegerType),
    StructField("contact_by_IVR", IntegerType),
    StructField("contact_by_teleCalling", IntegerType),
    StructField("dt", StringType)
  ))

  val LMSSchema = StructType(Seq(
    StructField("customer_id", StringType),
    StructField("account_details", StructType(
      Seq(
        StructField("AccountNumber", StringType),
        StructField("Status", IntegerType),
        StructField("TotalAmount", DoubleType),
        StructField("BalanceAmount", DoubleType),
        StructField("DueDate", TimestampType),
        StructField("LateFee", DoubleType)
      )
    )),
    StructField("created_at", TimestampType),
    StructField("updated_at", TimestampType),
    StructField("id", DoubleType),
    StructField("account_number", StringType),
    StructField("due_date", TimestampType),
    StructField("total_amount", DoubleType),
    StructField("balance_amount", DoubleType),
    StructField("late_fee", DoubleType),
    StructField("product_type", IntegerType),
    StructField("status", IntegerType),
    StructField("contact_by_SMS", IntegerType),
    StructField("contact_by_IVR", IntegerType),
    StructField("contact_by_teleCalling", IntegerType),
    StructField("dt", StringType)
  ))

  val PaymentsTxnSchema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("payment_type", StringType),
    StructField("order_item_id", StringType),
    StructField("selling_price", DoubleType),
    StructField("merchant_L2_type", StringType),
    StructField("merchant_L1_type", StringType),
    StructField("merchant_category", StringType),
    StructField("merchant_subcategory", StringType),
    StructField("mid", StringType),
    StructField("alipay_trans_id", StringType),
    StructField("byUPI", IntegerType),
    StructField("byNB", IntegerType),
    StructField("successfulTxnFlag", IntegerType),
    StructField("payment_mode", StringType),
    StructField("created_at", StringType),
    StructField("is_pg_drop_off", IntegerType),
    StructField("request_type", StringType),
    StructField("dt", DateType)
  ))
}
