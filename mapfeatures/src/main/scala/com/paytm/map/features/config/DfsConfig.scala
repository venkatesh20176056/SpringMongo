package com.paytm.map.features.config

import com.typesafe.config.Config

case class DfsConfig(
  rawTable: String,
  featuresTable: String,
  featuresPlusRoot: String,
  featuresPlusData: String,
  featuresPlusModel: String,
  featuresPlusFeatures: String,
  campaignsTable: String,
  campaignCustTable: String,
  canadaCampaigns: String,
  exportTable: String,
  metricTable: String,
  suppressionTable: String,
  profile: String,
  level: String,
  joined: String,
  values: String,
  resources: String,
  measurementPath: String,
  heroLogs: String,
  signalEvents: String,
  baseDFS: BaseDFSConfig,
  featuresTemp: String,
  schemaMigrationHistory: String,
  specSheets: String,
  featureList: String,
  lookAlike: String,
  urbanAirship: String,
  merchantFeatureList: String,
  merchantSpecSheets: String,
  merchantFlatTable: String,
  merchantCityStateMapping: String,
  ccAbuserMapping: String,
  cashbackAbuserMapping: String,
  ticketnewUsers: String,
  insiderUsers: String,
  shinraRankedCategories: String,
  pushCassandraDeviceInfo: String,
  pushCassandraUserToken: String,
  deviceIdPath: String,
  featureUsageReport: String,
  canadaFeatureList: String,
  weightedTrendFilteringCategories: String,
  weightedTrendFilteringTopCategories: String,
  measurementsDeviceAgg: String
)

object DfsConfig {

  def apply(config: Config): DfsConfig = {
    // extracted from config
    val rawTable: String = config.getString("dfs_features_raw")
    val featuresTable: String = config.getString("dfs_features_snapshot")
    val featuresPlus: String = config.getString("dfs_featuresplus_snapshot")
    val campaignsTable: String = config.getString("dfs_campaigns_update")
    val campaignCustTable: String = config.getString("dfs_campaign_customers")
    val canadaCampaigns: String = config.getString("dfs_canada_campaigns")
    val metricTable: String = config.getString("dfs_features_metric")
    val exportTable: String = config.getString("dfs_features_export")
    val suppressionTable: String = config.getString("dfs_suppression_list")
    val resourcesPath: String = config.getString("dfs_resources")
    val measurementsPath: String = config.getString("dfs_measurements")
    val heroLogs: String = config.getString("dfs_hero_logs")
    val signalEvents: String = config.getString("dfs_signal_events")
    val baseDFS: BaseDFSConfig = BaseDFSConfig(rawTable)
    val featuresTemp: String = config.getString("features_temp")
    val schemaMigrationHistory: String = config.getString("schema_migration_history")
    val lookAlike: String = config.getString("dfs_look_alike")
    val mapMeasurements: String = config.getString("dfs_map_measurements")
    val merchantMappings: String = config.getString("dfs_merchant_mappings")
    val ticketnewUsers: String = config.getString("dfs_ticketnew_users")
    val insiderUsers: String = config.getString("dfs_insider_users")
    val shinraRankedCategories: String = config.getString("dfs_shinra_ranked_categories")
    val pushCassandraDeviceInfo: String = config.getString("dfs_pushcassandra_deviceinfo")
    val pushCassandraUserToken: String = config.getString("dfs_pushcassandra_usertoken")
    val weightedTrendFilteringCategories: String = config.getString("dfs_weighted_trend_filtering_categories")
    val weightedTrendFilteringTopCategories: String = config.getString("dfs_weighted_trend_filtering_top_categories")
    val featureUsageReport: String = config.getString("dfs_feature_usage_report")

    // built here
    val profile: String = "profile/"
    val deviceIdPath: String = "deviceId/"

    val level: String = "levels/"
    val joined: String = "flatTable/"
    val merchantFlatTable: String = "merchantFlatTable/"
    val values: String = "values/"
    val specSheets: String = "SpecSheets/"
    val featureList: String = "feature_list_india_new.csv"
    val merchantSpecSheets: String = "SpecSheetsMerchant/"
    val merchantFeatureList: String = "feature_list_india_merchant.csv"
    val canadaFeatureList: String = "feature_list_canada.csv"
    val superCashback: String = "superCashback"
    val featuresPlusData: String = featuresPlus + "/data"
    val featuresPlusModel: String = featuresPlus + "/model"
    val featuresPlusFeatures: String = featuresPlus + "features"
    val urbanAirshipData: String = mapMeasurements + "push/urban_airship/"

    val merchantCityStateMapping: String = merchantMappings + "city_mapping/"
    val cashbackAbuserMapping: String = merchantMappings + "Fraud_merchants/"
    val ccAbuserMapping: String = merchantMappings + "P2P_Abuser_cust_ids/"

    val measurementsDeviceAgg: String = mapMeasurements + "deviceToCustomer_agg"

    DfsConfig(
      rawTable,
      featuresTable,
      featuresPlus,
      featuresPlusData,
      featuresPlusModel,
      featuresPlusFeatures,
      campaignsTable,
      campaignCustTable,
      canadaCampaigns,
      exportTable,
      metricTable,
      suppressionTable,
      profile,
      level,
      joined,
      values,
      resourcesPath,
      measurementsPath,
      heroLogs,
      signalEvents,
      baseDFS,
      featuresTemp,
      schemaMigrationHistory,
      specSheets,
      featureList,
      lookAlike,
      urbanAirshipData,
      merchantFeatureList,
      merchantSpecSheets,
      merchantFlatTable,
      merchantCityStateMapping,
      ccAbuserMapping,
      cashbackAbuserMapping,
      ticketnewUsers,
      insiderUsers,
      shinraRankedCategories,
      pushCassandraDeviceInfo,
      pushCassandraUserToken,
      deviceIdPath,
      featureUsageReport,
      canadaFeatureList,
      weightedTrendFilteringCategories,
      weightedTrendFilteringTopCategories,
      measurementsDeviceAgg
    )
  }
}
