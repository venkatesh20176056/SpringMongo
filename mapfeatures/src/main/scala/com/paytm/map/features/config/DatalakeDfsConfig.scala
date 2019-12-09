package com.paytm.map.features.config

import com.typesafe.config.Config
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import scala.io.Source.fromInputStream
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

case class IndexSchema(
  id: Int,
  name: String,
  dataset_type: String,
  ingest_type: String,
  description: String,
  status: String,
  slo_minutes: String,
  version: String,
  min_date: String,
  max_date: String,
  owner: String,
  retention_days: String,
  reconcile_mins: String,
  max_data_hours: String,
  last_ingest_time: String,
  ingest_threads_count: String,
  created_at: String,
  updated_at: String
)

object DatalakeDfsConfig {

  val bills = Seq("bills_adanigas", "bills_aircel", "bills_airtel", "bills_ajmerelectricity", "bills_apepdcl", "bills_assampower", "bills_bangalorewater", "bills_bescom", "bills_bescom_ebps", "bills_best", "bills_bhagalpurelectricity", "bills_bharatpurelectricity", "bills_bses", "bills_bses_bkup", "bills_bsnl", "bills_calcuttaelectric", "bills_cspdcl", "bills_delhijalboard", "bills_dishtv", "bills_dps", "bills_essel", "bills_gescom", "bills_haryanaelectricity", "bills_hescom", "bills_iciciinsurance", "bills_idea", "bills_igl", "bills_indiafirst", "bills_indiapower", "bills_jaipurelectricity", "bills_jodhpurelectricity", "bills_jusco", "bills_kotaelectricity", "bills_kseb", "bills_mahanagargas", "bills_manappuram", "bills_mangaloreelectricity", "bills_matrixpostpaid", "bills_meptolls", "bills_mpelectricity", "bills_msedcl", "bills_mumbaimetro", "bills_noidapower", "bills_northbiharpower", "bills_orrissadiscoms", "bills_pspcl", "bills_reliance", "bills_relianceenergy", "bills_relianceinsurance", "bills_reliancemobile", "bills_sitienergy", "bills_southbiharpower", "bills_t24", "bills_tata_ebps", "bills_tatadocomo", "bills_tataindicom", "bills_tatalandline", "bills_tataphoton", "bills_tatapower", "bills_tatapowermumbai", "bills_telanganapower", "bills_torrentpower", "bills_tsnpdcl", "bills_uppcl", "bills_vodafone", "bills_wbsedcl")

  // TODO: Revert this
  def getDataSetIds(query: String): List[IndexSchema] = {
    val snapshotQueryEndPoint: String = s"$query?use_defaults=false"

    implicit val formats = DefaultFormats
    val httpClient = HttpClientBuilder.create.build
    val httpResponse = httpClient.execute(new HttpGet(s"$snapshotQueryEndPoint"))
    val responseCode = httpResponse.getStatusLine.getStatusCode
    if (responseCode == 200) {
      val inputStream = httpResponse.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      JsonMethods.parse(content).extract[List[IndexSchema]]
    } else {
      val message = s"Error in retrieving the all dataset ids with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

  def getLatestSnapshotV3(datasetId: Int, snapshotQueryEndPoint: String): Map[String, String] = {
    val query = s"$snapshotQueryEndPoint$datasetId/manifest/"
    getResponseAsMap(query)
  }

  def getLatestSnapshotV3ByName(datasetName: String, snapshotQueryEndPoint: String): Map[String, String] = {
    val query = s"$snapshotQueryEndPoint$datasetName/manifest/"
    getResponseAsMap(query)
  }

  def getLatestSnapshotV3ByNameAsArray(datasetName: String, snapshotQueryEndPoint: String): Array[String] = {
    val query = s"$snapshotQueryEndPoint$datasetName/manifest/"
    getResponseAsArray(query)
  }

  private def getResponseAsMap(query: String): Map[String, String] = {
    implicit val formats = DefaultFormats
    val httpClient = HttpClientBuilder.create.build
    val httpResponse = httpClient.execute(new HttpGet(s"$query"))
    val responseCode = httpResponse.getStatusLine.getStatusCode
    if (responseCode == 200) {
      val inputStream = httpResponse.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      (JsonMethods.parse(content) \ "partitions").extract[Map[String, String]]
    } else {
      val message = s"Error in retrieving the latest snapshots for dataset id/name = ${query.split("/").takeRight(2).head} in \'get dataset is not working\' with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

  private def getResponseAsArray(query: String): Array[String] = {
    implicit val formats = DefaultFormats
    val httpClient = HttpClientBuilder.create.build
    val httpResponse = httpClient.execute(new HttpGet(s"$query"))
    val responseCode = httpResponse.getStatusLine.getStatusCode
    if (responseCode == 200) {
      val inputStream = httpResponse.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      val snapshots: Map[String, String] = (JsonMethods.parse(content) \ "partitions").extract[Map[String, String]]
      snapshots.values.toArray.map(x => x.replace("s3://", "s3a://"))
    } else {
      val message = s"Error in retrieving the latest snapshots for dataset id/name = ${query.split("/").takeRight(2).head} in \'get dataset is not working\' with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

  private val getId = (datasetsId: List[IndexSchema], dbName: String, tableName: String) => {
    val out: List[Int] = datasetsId.filter(_.name.toLowerCase == s"$dbName.$tableName").map(_.id)
    if (out.nonEmpty) {
      out.head
    } else {
      val message = s"Error in retrieving the latest snapshots for $dbName.$tableName as it returns empty list."
      throw new RuntimeException(message)
    }
  }

  private def getSnapshotName(snapshotQueryEndPoint: String, databaseName: String): String = {

    implicit val formats = DefaultFormats
    val httpClient = HttpClientBuilder.create.build
    val httpResponse = httpClient.execute(new HttpGet(s"$snapshotQueryEndPoint/$databaseName"))
    val responseCode = httpResponse.getStatusLine.getStatusCode

    if (responseCode == 200) {
      val inputStream = httpResponse.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString

      inputStream.close()
      httpClient.close()

      (JsonMethods.parse(content) \ "snapshot_name").extract[String]
    } else {
      val message = s"Error in retrieving the latest snapshot name for $databaseName with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

  private def getSnapshotDirectoryOf(keyName: String, config: Config): String = {
    val basePath = config.getString("base")
    val snapshotQueryEndPoint = config.getString("snapshotQueryEndPoint")
    val databaseName = config.getString(keyName)
    val snapshotName = getSnapshotName(snapshotQueryEndPoint, databaseName)

    s"$basePath/$databaseName.db/.snapshot/$snapshotName/"
  }

  def apply(config: Config) = {
    val snapshotQueryEndPointV3 = config.getString("snapshotQueryEndPointV3")
    val datasetsId: List[IndexSchema] = getDataSetIds(snapshotQueryEndPointV3)

    // getting all the databases name:
    val oauth: String = config.getString("oauth")
    val kyc: String = config.getString("kyc")
    val pd: String = config.getString("pd")
    val msBase: String = config.getString("ms_base")

    // If new tables are added, please add them in wiki too
    // https://wiki.mypaytm.com/display/MAP/Input+Datasets
    class DatalakeDfsConfigClass() {

      val daaSQueryEndPoint = snapshotQueryEndPointV3

      lazy val custRegnPath: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"$oauth", s"customer_registration"), snapshotQueryEndPointV3)
      lazy val kycUserDetailPath: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"$kyc", s"kyc_user_detail"), snapshotQueryEndPointV3)
      lazy val pushLogPath: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"$pd", s"push_logs"), snapshotQueryEndPointV3)
      lazy val salesOrder: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, "marketplace", s"sales_order"), snapshotQueryEndPointV3)
      lazy val salesOrderItem: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, "marketplace", s"sales_order_item"), snapshotQueryEndPointV3)
      lazy val salesOrderPayment: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, "marketplace", s"sales_order_payment"), snapshotQueryEndPointV3)
      lazy val salesOrderAdd: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, "marketplace", s"sales_order_address"), snapshotQueryEndPointV3)
      lazy val salesOrderMetadata: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"marketplace", s"sales_order_metadata"), snapshotQueryEndPointV3)
      lazy val salesOrderFulfillment: Map[String, String] = getLatestSnapshotV3ByName("marketplace.sales_order_fulfillment", snapshotQueryEndPointV3)
      lazy val promoCodeUsage: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"code", s"promocode_usage"), snapshotQueryEndPointV3)
      lazy val catalog_category: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"marketplace", s"catalog_category"), snapshotQueryEndPointV3)
      lazy val catalog_product: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"marketplace", s"catalog_product"), snapshotQueryEndPointV3)
      lazy val fulfillementRecharge: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"fs_recharge", s"fulfillment_recharge"), snapshotQueryEndPointV3)
      lazy val catalogVerticalRecharge: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"fs_recharge", s"catalog_vertical_recharge"), snapshotQueryEndPointV3)
      lazy val catalog_entity_decorator: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"marketplace_catalog", s"catalog_entity_decorator"), snapshotQueryEndPointV3)
      lazy val newSystemTxnRequest: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"new_system_txn_request"), snapshotQueryEndPointV3)
      lazy val merchantUser: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"merchant"), snapshotQueryEndPointV3)
      lazy val walletUser: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"user"), snapshotQueryEndPointV3)
      lazy val merchantBank: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"merchant_bank_details"), snapshotQueryEndPointV3)
      lazy val walletBankTxn: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"new_bank_txn_details"), snapshotQueryEndPointV3)
      lazy val billsPaths: Array[String] = bills.map(x => getLatestSnapshotV3ByNameAsArray(s"digital_reminder.$x", snapshotQueryEndPointV3)).reduce(_ ++ _)
      lazy val entityDemographics: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"pg", s"entity_demographics"), snapshotQueryEndPointV3)
      lazy val entityInfo: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"pg", s"entity_info"), snapshotQueryEndPointV3)
      lazy val uidEidMapper: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"pg", s"uid_eid_mapper"), snapshotQueryEndPointV3)
      lazy val entityPreferences = getLatestSnapshotV3(getId(datasetsId, s"pg", s"entity_preferences_info"), snapshotQueryEndPointV3)
      lazy val physicalDebitCard = getLatestSnapshotV3(getId(datasetsId, s"bank_product", s"physical_debit_card"), snapshotQueryEndPointV3)
      lazy val IDCDetails = getLatestSnapshotV3(getId(datasetsId, s"bank_product", s"idc_details"), snapshotQueryEndPointV3)
      lazy val TBAADMGeneralAcctMast = getLatestSnapshotV3(getId(datasetsId, s"cbsprd", s"tbaadm_general_acct_mast_table"), snapshotQueryEndPointV3)
      lazy val TBAADMHistTranDtl = getLatestSnapshotV3(getId(datasetsId, s"cbsprd", s"custom_dtd_dtlake"), snapshotQueryEndPointV3)
      lazy val walletDetails = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"wallet_details"), snapshotQueryEndPointV3)
      lazy val prepaidExpiry: Array[String] = getLatestSnapshotV3ByNameAsArray(s"recharge_analytics.plan_validity", snapshotQueryEndPointV3)
      lazy val dwhCustRegnPath: Map[String, String] = getLatestSnapshotV3ByName(s"dwh.customer_registration_snapshot", snapshotQueryEndPointV3)
      lazy val paytmAPlusMapping: Map[String, String] = getLatestSnapshotV3ByName(s"dwh.adm_cu_common_unique_key_snapshot", snapshotQueryEndPointV3)
      lazy val lmsAccount: Map[String, String] = getLatestSnapshotV3ByName(s"recovery_collections.account_postpaid", snapshotQueryEndPointV3)
      lazy val lmsContactability: Map[String, String] = getLatestSnapshotV3ByName(s"recovery_collections.contactibility", snapshotQueryEndPointV3)
      lazy val lmsAccountV2: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, "recovery_collectionsv2", "accounts"), snapshotQueryEndPointV3)
      lazy val lmsCaseV2: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"recovery_collectionsv2", "cases"), snapshotQueryEndPointV3)
      lazy val lmsContactabilityV2: Map[String, String] = getLatestSnapshotV3ByName(s"recovery_collectionsv2.contactibility", snapshotQueryEndPointV3)
      lazy val offRetailer: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"dwh", s"offline_retailer"), snapshotQueryEndPointV3)

      // Merchant
      lazy val merchantSupercash: Map[String, String] = getLatestSnapshotV3ByName(s"supercash_merchant.promo_supercash", snapshotQueryEndPointV3)
      lazy val offOrgMerchant: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"datalake", s"offline_organized_merchant"), snapshotQueryEndPointV3)
      lazy val factOffMerchant: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"datalake", s"fact_offline_merchant"), snapshotQueryEndPointV3)
      lazy val entityEDCInfo: Map[String, String] = getLatestSnapshotV3ByName("paytmpgdb.entity_edc_info", snapshotQueryEndPointV3)
      lazy val merchantOnboardingVubm: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.v_ubm", snapshotQueryEndPointV3)
      lazy val merchantOnboardingRelatedBusinessSolution: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.related_business_solution_mapping", snapshotQueryEndPointV3)
      lazy val merchantOnboardingSolutionAdditionalInfo: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.solution_additional_info", snapshotQueryEndPointV3)
      lazy val merchantOnboardingWorkflowStatus: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.workflow_status", snapshotQueryEndPointV3)
      lazy val merchantOnboardingWorkflowNode: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.workflow_node", snapshotQueryEndPointV3)
      lazy val merchantOnboardingSolutionDoc: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.solution_doc", snapshotQueryEndPointV3)
      lazy val merchantOnboardingFieldRejectionReason: Map[String, String] = getLatestSnapshotV3ByName("onboarding_engine.field_rejection_reason", snapshotQueryEndPointV3)
      lazy val admAuditRecord: Map[String, String] = getLatestSnapshotV3ByName("dwh.adm_audit_record_snapshot", snapshotQueryEndPointV3)
      lazy val admCuCommonUniqueKey: Map[String, String] = getLatestSnapshotV3ByName("dwh.adm_cu_common_unique_key_snapshot", snapshotQueryEndPointV3)

      // Old UPI Tables
      lazy val upiUser: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"switch", s"upi_user"), snapshotQueryEndPointV3)
      lazy val upiVirtualAddress: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"switch", "upi_customer_virtual_address"), snapshotQueryEndPointV3)
      lazy val upiBankAccounts: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"switch", "upi_customer_bank_accounts"), snapshotQueryEndPointV3)
      lazy val upiDeviceBound: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"switch", "upi_device_binding"), snapshotQueryEndPointV3)
      lazy val upiTranLog: Map[String, String] = getLatestSnapshotV3ByName("switch.upi_tranlog", snapshotQueryEndPointV3)
      // UPI V2 Tables
      lazy val upiUserV2: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_user", snapshotQueryEndPointV3)
      lazy val upiVirtualAddressV2: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_user_vpa", snapshotQueryEndPointV3)
      lazy val upiBankAccountsV2: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_account", snapshotQueryEndPointV3)
      lazy val upiDeviceBoundV2: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_device_registration", snapshotQueryEndPointV3)
      lazy val upiTxnInfo: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_txn_info", snapshotQueryEndPointV3)
      lazy val upiTxnParticipants: Map[String, String] = getLatestSnapshotV3ByName("switchv2.v_datalake_txn_participants", snapshotQueryEndPointV3)

      // SMS Tables
      lazy val smsTravel: Array[String] = getLatestSnapshotV3ByNameAsArray(s"sms_data.travel", snapshotQueryEndPointV3)
      lazy val smsUserBills: Array[String] = getLatestSnapshotV3ByNameAsArray(s"sms_data.user_bills", snapshotQueryEndPointV3)
      lazy val smsPurchases: Array[String] = getLatestSnapshotV3ByNameAsArray(s"sms_data.purchases", snapshotQueryEndPointV3)

      lazy val superCashbacks: Map[String, String] = getLatestSnapshotV3ByName(s"promocard.promo_supercash", snapshotQueryEndPointV3)
      lazy val campaigns: Map[String, String] = getLatestSnapshotV3ByName(s"code.campaign", snapshotQueryEndPointV3)
      lazy val userAttributesSnapshot: Map[String, String] = getLatestSnapshotV3ByName(s"dwh.user_attributes_snapshot", snapshotQueryEndPointV3)

      lazy val alipayPgOlap: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"dwh", s"alipay_pg_olap"), snapshotQueryEndPointV3)
      lazy val offlineRFSE: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"dwh", s"offline_rfse"), snapshotQueryEndPointV3)

      lazy val goldBuyOrders: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wealthmgmt", s"dg_buy_orders"), snapshotQueryEndPointV3)
      lazy val goldSellOrders: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wealthmgmt", s"dg_sell_orders"), snapshotQueryEndPointV3)
      lazy val goldBackOrders: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wealthmgmt", s"dg_redeem_orders"), snapshotQueryEndPointV3)
      lazy val goldWallet: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wealthmgmt", s"customer_portfolio"), snapshotQueryEndPointV3)

      lazy val leadsTable: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"leadgen", s"leads"), snapshotQueryEndPointV3)
      lazy val digitalCreditAccounts: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"accounts"), snapshotQueryEndPointV3)
      lazy val digitalCreditEligibleUsers: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"eligible_users"), snapshotQueryEndPointV3)
      lazy val digitalCreditEarlyAccess: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"early_access"), snapshotQueryEndPointV3)
      lazy val digitalCreditBills: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"bills"), snapshotQueryEndPointV3)
      lazy val digitalCreditStatements: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"statement"), snapshotQueryEndPointV3)
      lazy val digitalCreditBankInitiated: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"bank_initiated_transactions"), snapshotQueryEndPointV3)
      lazy val digitalCreditRevolve: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"revolve"), snapshotQueryEndPointV3)
      lazy val digitalCreditSubscriptions: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"subscriptions"), snapshotQueryEndPointV3)
      lazy val digitalCreditEligibleUsersReporting: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"paytm_digital_credit", s"eligible_users_reporting"), snapshotQueryEndPointV3)

      lazy val postPaidAcquisitionModelV2: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"fs_mycroft", s"postpaid_acquisition_model_v2"), snapshotQueryEndPointV3)
      lazy val postPaidAdoptionScore: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"fs_mycroft", s"postpaid_adoption_score"), snapshotQueryEndPointV3)
      lazy val postPaidDeferEligible: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"fs_mycroft", s"postpaid_pepl_defer_data"), snapshotQueryEndPointV3)

      lazy val gaAppPaths = getLatestSnapshotV3(getId(datasetsId, s"ga", s"ga_app_identified"), snapshotQueryEndPointV3)
      lazy val gaWebPaths = getLatestSnapshotV3(getId(datasetsId, s"ga", s"ga_web"), snapshotQueryEndPointV3)
      lazy val gaMallAppPaths = getLatestSnapshotV3(getId(datasetsId, s"ga", s"ga_mall_app"), snapshotQueryEndPointV3)
      lazy val gaMallWebPaths = getLatestSnapshotV3(getId(datasetsId, s"ga", s"ga_mall_web"), snapshotQueryEndPointV3)

      lazy val midgarAuditLogs = s"$msBase/events/midgar_service_audit_log"
      lazy val corporate_merchants: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"datalake", s"corporate_merchants"), snapshotQueryEndPointV3)
      lazy val ppi_details: Map[String, String] = getLatestSnapshotV3(getId(datasetsId, s"wallet", s"ppi_details"), snapshotQueryEndPointV3)
      lazy val flights = getLatestSnapshotV3ByName(s"dwh.flights_olap", snapshotQueryEndPointV3)
      lazy val trains = getLatestSnapshotV3ByName(s"dwh.trains_olap", snapshotQueryEndPointV3)
      lazy val trainsPnrCheck = getLatestSnapshotV3ByName(s"datastore_travel_trains.pnr_check_customer_data", snapshotQueryEndPointV3)
      lazy val buses = getLatestSnapshotV3ByName(s"dwh.bus_olap", snapshotQueryEndPointV3)

      lazy val customerOlap = getLatestSnapshotV3(getId(datasetsId, s"dwh", s"kyc_customer_olap"), snapshotQueryEndPointV3)

      // Gamepind
      lazy val gamepindTrans = getLatestSnapshotV3ByName("agt_pcs.trans", snapshotQueryEndPointV3)
      lazy val gamepindUserAccount = getLatestSnapshotV3ByName("agt_pcs.user_account", snapshotQueryEndPointV3)
      lazy val gamepindRunningTab = getLatestSnapshotV3ByName("agt_pcs.running_tab", snapshotQueryEndPointV3)
      lazy val ppProfilerGameServeHistory = getLatestSnapshotV3ByName("pp_profiler.game_serve_history", snapshotQueryEndPointV3)
      lazy val casUser = getLatestSnapshotV3ByName("cas.user", snapshotQueryEndPointV3)
      lazy val battleCenterPlayerBillingHistory = getLatestSnapshotV3ByName("battlecenter.player_billing_history", snapshotQueryEndPointV3)
      lazy val battleCenterBattleTxn = getLatestSnapshotV3ByName("battlecenter.battle_txn", snapshotQueryEndPointV3)
      lazy val battleCenterWinners = getLatestSnapshotV3ByName("battlecenter.winners", snapshotQueryEndPointV3)
      lazy val gamepindTransactions = getLatestSnapshotV3ByName("dwh.gamepind_transactions", snapshotQueryEndPointV3)
      lazy val taskManagerTransactionalLogging = getLatestSnapshotV3ByName("taskmanager.transactional_logging", snapshotQueryEndPointV3)
      lazy val spsCreditData = getLatestSnapshotV3ByName("sps-credit.credit_data", snapshotQueryEndPointV3)
      lazy val spsBillingData = getLatestSnapshotV3ByName("sps.billing_data", snapshotQueryEndPointV3)
      lazy val xengageWeUserGcm = getLatestSnapshotV3ByName("xengage.we_user_gcm", snapshotQueryEndPointV3)
      lazy val rummyRcgGamePlayer = getLatestSnapshotV3ByName("rummy.rcg_game_player", snapshotQueryEndPointV3)

      // Movies
      lazy val moviesData = getLatestSnapshotV3ByName("movies.movie_data", snapshotQueryEndPointV3)

      lazy val offlineTxnFacts: Map[String, String] = getLatestSnapshotV3ByName("datalake.fact_offus", snapshotQueryEndPointV3)
      lazy val pgTxnInfo: Map[String, String] = getLatestSnapshotV3ByName("pg.txn_info", snapshotQueryEndPointV3)
      lazy val pgTxnMetaInfo: Map[String, String] = getLatestSnapshotV3ByName("pg.txn_meta_info", snapshotQueryEndPointV3)
      lazy val pgLookup: Map[String, String] = getLatestSnapshotV3ByName("pg.lookup_data", snapshotQueryEndPointV3)
      lazy val pgTxnCardInfo: Map[String, String] = getLatestSnapshotV3ByName("pg.txn_card_info", snapshotQueryEndPointV3)

      lazy val paytmPrime = getLatestSnapshotV3ByName("paytm_prime.prime_users", snapshotQueryEndPointV3)

      lazy val factCustomer = getLatestSnapshotV3ByName("datalake.fact_customer", snapshotQueryEndPointV3)

    }

    val DatalakeDfsConfig = new DatalakeDfsConfigClass

    DatalakeDfsConfig
  }

}
