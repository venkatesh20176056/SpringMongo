package com.paytm.map.featuresplus.genderestimation

object GEModelConfigs {

  val modelName = "genderEstimation"
  val featureName = "genderPlus"
  val femaleLabel = 2.0
  val maleLabel = 1.0

  def getEC2Features(cols: Seq[String]): Seq[String] = {
    cols.filter(x =>
      x.contains("EC2")
        && (x.matches(".*_total_clp_sessions_app")
          || x.matches(".*_total_clp_sessions_web")))
  }

  val otherFeatures = Seq(
    "customer_id",
    "gender",
    "EC1_MF_total_clp_sessions_app",
    "EC1_MF_total_clp_sessions_app_30_days",
    "EC1_MF_total_clp_sessions_app_60_days",
    "EC1_MF_total_clp_sessions_web",
    "EC1_MF_total_clp_sessions_web_30_days",
    "EC1_MF_total_clp_sessions_web_60_days",
    "EC1_MF_total_glp_sessions_app",
    "EC1_MF_total_glp_sessions_app_30_days",
    "EC1_MF_total_glp_sessions_app_60_days",
    "EC1_MF_total_glp_sessions_web",
    "EC1_MF_total_glp_sessions_web_30_days",
    "EC1_MF_total_glp_sessions_web_60_days",
    "EC1_MF_total_product_views_app",
    "EC1_MF_total_product_views_app_30_days",
    "EC1_MF_total_product_views_app_60_days",
    "EC1_MF_total_product_clicks_app",
    "EC1_MF_total_product_clicks_app_30_days",
    "EC1_MF_total_product_clicks_app_60_days",
    "EC1_MF_total_product_clicks_web",
    "EC1_MF_total_product_clicks_web_30_days",
    "EC1_MF_total_product_clicks_web_60_days",
    "EC1_MF_total_transaction_count",
    "EC1_MF_total_transaction_count_1_days",
    "EC1_MF_total_transaction_count_3_days",
    "EC1_MF_total_transaction_count_7_days",
    "EC1_MF_total_transaction_count_30_days",
    "EC1_MF_total_transaction_count_90_days",
    "EC1_MF_total_transaction_count_180_days",
    "EC1_WF_total_clp_sessions_app",
    "EC1_WF_total_clp_sessions_app_30_days",
    "EC1_WF_total_clp_sessions_app_60_days",
    "EC1_WF_total_clp_sessions_web",
    "EC1_WF_total_clp_sessions_web_30_days",
    "EC1_WF_total_clp_sessions_web_60_days",
    "EC1_WF_total_glp_sessions_app",
    "EC1_WF_total_glp_sessions_app_30_days",
    "EC1_WF_total_glp_sessions_app_60_days",
    "EC1_WF_total_glp_sessions_web",
    "EC1_WF_total_glp_sessions_web_30_days",
    "EC1_WF_total_glp_sessions_web_60_days",
    "EC1_WF_total_product_views_app",
    "EC1_WF_total_product_views_app_30_days",
    "EC1_WF_total_product_views_app_60_days",
    "EC1_WF_total_product_clicks_app",
    "EC1_WF_total_product_clicks_app_30_days",
    "EC1_WF_total_product_clicks_app_60_days",
    "EC1_WF_total_product_clicks_web",
    "EC1_WF_total_product_clicks_web_30_days",
    "EC1_WF_total_product_clicks_web_60_days",
    "EC1_WF_total_transaction_count",
    "EC1_WF_total_transaction_count_1_days",
    "EC1_WF_total_transaction_count_3_days",
    "EC1_WF_total_transaction_count_7_days",
    "EC1_WF_total_transaction_count_30_days",
    "EC1_WF_total_transaction_count_90_days",
    "EC1_WF_total_transaction_count_180_days",
    "RU_MB_prepaid_total_transaction_count_1_days",
    "RU_MB_prepaid_total_transaction_count_3_days",
    "RU_MB_prepaid_total_transaction_count_7_days",
    "RU_MB_prepaid_total_transaction_count_30_days",
    "RU_MB_prepaid_total_transaction_count_90_days",
    "RU_MB_prepaid_total_transaction_count_180_days",
    "RU_MB_prepaid_total_transaction_size_1_days",
    "RU_MB_prepaid_total_transaction_size_3_days",
    "RU_MB_prepaid_total_transaction_size_7_days",
    "RU_MB_prepaid_total_transaction_size_30_days",
    "RU_MB_prepaid_total_transaction_size_90_days",
    "RU_MB_prepaid_total_transaction_size_180_days"
  )
}