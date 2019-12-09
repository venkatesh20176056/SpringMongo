package com.paytm.map.features.featurevalue

import com.paytm.map.features.datasets.Constants.groupList
import com.paytm.map.features.utils.Settings

sealed trait FeatureValueConfig {
  def getInputPaths(settings: Settings, targetDateStr: String): Seq[String]

  def getOutputPath(settings: Settings): String

  def getColumnsToExclude: Seq[String]
}

object FeatureValueMerchant extends FeatureValueConfig {
  def getInputPaths(settings: Settings, targetDateStr: String): Seq[String] = Seq(s"${settings.featuresDfs.featuresTable}${settings.featuresDfs.merchantFlatTable}/dt=${targetDateStr}")

  def getOutputPath(settings: Settings): String = s"${settings.featuresDfs.featuresTable}/merchant/values"
  def getColumnsToExclude: Seq[String] = Seq(
    "merchant_id",
    "phone_no",
    "secondary_contact_phone_no"
  )
}

object FeatureValueHero extends FeatureValueConfig {
  def getInputPaths(settings: Settings, targetDateStr: String): Seq[String] = Seq(s"${settings.featuresDfs.featuresTable}/hero/${settings.featuresDfs.joined}/dt=$targetDateStr")

  def getOutputPath(settings: Settings): String = s"${settings.featuresDfs.featuresTable}/hero/${settings.featuresDfs.values}"

  def getColumnsToExclude: Seq[String] = Seq(
    "email", "email_id", "first_name",
    "last_name", "referrer_code", "number",
    "created_orders_biller_name_0_days",
    "created_orders_biller_name_1_days",
    "created_orders_biller_name_2_days",
    "created_orders_gift_card_name_amount_0_days",
    "created_orders_gift_card_name_amount_1_days",
    "created_orders_gift_card_name_amount_2_days"
  )
}

object FeatureValueIndia extends FeatureValueConfig {
  def getInputPaths(settings: Settings, targetDateStr: String) = {
    val groupNames = groupList.dropRight(1) //Remove group 4 as there are no features there
    groupNames.map(groupName => s"${settings.featuresDfs.featuresTable}/$groupName/${settings.featuresDfs.joined}/dt=$targetDateStr")
  }

  def getOutputPath(settings: Settings): String = s"${settings.featuresDfs.featuresTable}/india/${settings.featuresDfs.values}"

  def getColumnsToExclude: Seq[String] = Seq(
    "customer_id",
    "email_id",
    "phone_no",
    "first_name",
    "full_name",
    "WALLET_P2P_all_transaction_custID_list",
    "mid",
    "merchant_first_name",
    "merchant_last_name",
    "merchant_email_ID"
  )

}
