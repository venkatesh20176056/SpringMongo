package com.paytm.map.features.Chakki.FeatChakki

import org.apache.spark.sql.types.{ArrayType, _}
import com.paytm.map.features.config.Schemas.SchemaRepo._
import com.paytm.map.features.config.Schemas.ECSchema._
import com.paytm.map.features.config.Schemas.SchemaRepoGA.{GAL3EC1LPSchema, GAL3EC2LPSchema, GAL3EC3LPSchema, GAL3EC3Schema}
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC2._
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC4._
import com.paytm.map.features.config.Schemas.FewMoreSchema._
import com.paytm.map.features.config.Schemas.SchemaRepo

object Constants {

  //Map to Store LogicalGroup to BaseTable Path
  def level2BaseTable(level: String): String = {
    val levelMap = Map(
      "BANK" -> "BankFeatures",
      "GABK" -> "GAFeatures/BK",
      "GAL2" -> "GAFeatures/L2",
      "GAL2" -> "GAFeatures/L2",
      "GAL2RU" -> "GAFeatures/L2RU",
      "GAL3BK" -> "GAFeatures/L3BK",
      "GAL3EC1" -> "GAFeatures/L3EC1",
      "GAL3EC2" -> "GAFeatures/L3EC2",
      "GAL3EC3" -> "GAFeatures/L3EC3",
      "GAL3EC4" -> "GAFeatures/L3EC4",
      "GAL3EC4" -> "GAFeatures/L3EC4",
      "GAL3EC4" -> "GAFeatures/L3EC4",
      "GAL3RU" -> "GAFeatures/L3RU",
      "GAPAYTM" -> "GAFeatures/L1",
      "GAGOLD" -> "GAFeatures/GOLD",
      "LMS" -> "lms",
      "LMSV2" -> "lmsV2",
      "PAYMENTS" -> "PaymentFeatures",
      "ONLINE" -> "OnlineFeatures",
      "PAYTM" -> "L1",
      "WALLET" -> "WalletFeatures",
      "SCB" -> "superCashback",
      "UPI" -> "UPI",
      "LS" -> "LastSeenAgg",
      "PUSH" -> "PushFeatures",
      "GAMEPIND" -> "GamepindFeatures",
      "GOLD" -> "GoldFeatures",
      "MS" -> "MerchantSales",
      "ML2" -> "MerchantL2",
      "MSC" -> "MerchantSuperCashback",
      "MGA" -> "MerchantGAFeatures",
      "POSTPAID" -> "PostPaid_Date",
      "MEDICAL" -> "MedicalFeaturesCustomer",
      "DEVICE" -> "DeviceFeatures",
      "SIGNAL" -> "SignalFeatures",
      "LOCATION" -> "LocationFeatures",
      "INSURANCE" -> "InsuranceFeatures",
      "OVERALL" -> "OverallFeatures"
    )
    levelMap.getOrElse(level, level)
  }

  //Map to Store LogicalGroup to WritePath
  def execLevel2WritePath(execLevel: String): String = {
    val execLevelMap = Map(
      "BANK" -> "bank",
      "LMS" -> "LMS_POSTPAID",
      "LMSV2" -> "LMS_POSTPAIDV2",
      "PAYMENTS" -> "payments",
      "ONLINE" -> "online",
      "WALLET" -> "wallet",
      "L3BK_EXTRA" -> "L3BK",
      "L3RU_EXTRA" -> "L3RU",
      "GOLD" -> "gold"
    )
    execLevelMap.getOrElse(execLevel, execLevel)
  }

  // Map to Store Execution Group to Logical Group
  def execLevel2Level(execLevel: String): String = {
    val execLevelMap = Map(
      "BK" -> "L2",
      "EC" -> "L2",
      "GAL2BK" -> "GAL2",
      "GAL2EC" -> "GAL2",
      "GAL3EC4_I" -> "GAL3EC4",
      "GAL3EC4_II" -> "GAL3EC4",
      "GAL3EC4_III" -> "GAL3EC4",
      "L3EC2_I" -> "L3EC2",
      "L3EC2_II" -> "L3EC2",
      "L3EC4_I" -> "L3EC4",
      "L3EC4_II" -> "L3EC4",
      "L3EC4_III" -> "L3EC4",
      "RU" -> "L2",
      "L3BK_EXTRA" -> "L3BK",
      "L3RU_EXTRA" -> "L3RU"
    )
    execLevelMap.getOrElse(execLevel, execLevel)
  }

  // DataType Map for easy storage
  val dId2DataType = Map(
    "int" -> IntegerType,
    "long" -> LongType,
    "string" -> StringType,
    "double" -> DoubleType,
    "date" -> DateType,
    "timestamp" -> TimestampType,
    "arr-str" -> ArrayType(StringType),
    "arr-long" -> ArrayType(LongType),
    "promoType" -> ArrayType(StructType(Seq(StructField("Promocode", StringType), StructField("Date", DateType)))),
    "operatorType" -> ArrayType(ArrayType(StructType(Seq(StructField("Operator", StringType), StructField("count", LongType))))),
    "lmsV2Type" -> ArrayType(StructType(
      Seq(
        StructField("AccountNumber", StringType),
        StructField("TotalAmount", DoubleType),
        StructField("BalanceAmount", DoubleType),
        StructField("DueDate", TimestampType),
        StructField("LateFee", DoubleType),
        StructField("ProductSource", StringType),
        StructField("ProductType", StringType),
        StructField("Status", IntegerType),
        StructField("DPD", IntegerType)
      )
    )),
    "lmsType" -> ArrayType(StructType(
      Seq(
        StructField("AccountNumber", StringType),
        StructField("Status", IntegerType),
        StructField("TotalAmount", DoubleType),
        StructField("BalanceAmount", DoubleType),
        StructField("DueDate", TimestampType),
        StructField("LateFee", DoubleType)
      )
    )),
    "smsType" -> ArrayType(StructType(
      Seq(
        StructField("SenderName", StringType),
        StructField("SentTimestamp", TimestampType)
      )
    )),
    "cashback_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_unAck_count", LongType),
        StructField("current_initialized_count", LongType),
        StructField("current_inprogress_count", LongType),
        StructField("current_completed_count", LongType),
        StructField("current_expired_count", LongType),
        StructField("total_instance_count", LongType),
        StructField("last_camp_completed_date", DateType),
        StructField("last_camp_expired_date", DateType),
        StructField("current_init_inprog_start_date", DateType),
        StructField("current_init_inprog_expiry_date", DateType),
        StructField("current_inprogress_stage", LongType),
        StructField("current_inprog_last_txn_date", DateType)
      )
    )),
    "cashback_unAck_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_unAck_count", LongType)
      )
    )),
    "cashback_initialized_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_initialized_count", LongType)
      )
    )),
    "cashback_inprogress_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_inprogress_count", LongType)
      )
    )),
    "cashback_completed_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_completed_count", LongType)
      )
    )),
    "cashback_expired_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_expired_count", LongType)
      )
    )),
    "cashback_total_instance_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("total_instance_count", LongType)
      )
    )),
    "cashback_last_camp_completed_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("last_camp_completed_date", DateType)
      )
    )),
    "cashback_last_camp_expired_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_expired_count", LongType)
      )
    )),
    "cashback_current_init_inprog_start_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_init_inprog_start_date", DateType)
      )
    )),
    "cashback_current_init_inprog_expiry_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_init_inprog_expiry_date", DateType)
      )
    )),
    "cashback_current_inprogress_stage_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_inprogress_stage", LongType)
      )
    )),
    "cashback_current_inprogress_last_txn_type" -> ArrayType(StructType(
      Seq(
        StructField("campaignName", StringType),
        StructField("current_inprog_last_txn_date", DateType)
      )
    )),
    "last_balance_credited_date_type" -> ArrayType(StructType(
      Seq(
        StructField("ppi_type", StringType),
        StructField("issuer_id", LongType),
        StructField("last_balance_credited_dt", DateType)
      )
    )),
    "subwallet_balance_type" -> ArrayType(StructType(
      Seq(
        StructField("ppi_type", StringType),
        StructField("issuer_id", LongType),
        StructField("balance", DoubleType)
      )
    )),
    "bankCardListType" -> SchemaRepo.bankCardStructSetType,
    "travelDetailListType" -> SchemaRepo.travelDetailStructSetType,
    "cinemaVisitedListType" -> SchemaRepo.cinemaVisitedStructSetType,
    "cineplexVisitedListType" -> SchemaRepo.cineplexVisitedStructSetType,
    "moviePassListType" -> SchemaRepo.moviePassStructSetType,
    "categorizedTxnsListType" -> SchemaRepo.categorizedTxnsListType,
    "merchantSuperCashStatusListType" -> SchemaRepo.merchantSuperCashStatusListType,
    "insuranceCategoryCountListType" -> SchemaRepo.insuranceCategoryCountListType,
    "insuranceCategoryTxnListType" -> SchemaRepo.insuranceCategoryTxnListType
  )

  //execLevel2LookBackDaya
  def level2LookBack(level: String): Option[Int] = {
    val levelMap = Map(
      "GABK" -> 180,
      "GAL2BK" -> 90,
      "GAL2EC" -> 90,
      "GAL2RU" -> 90,
      "GAL3BK" -> 90,
      "GAL3EC1" -> 90,
      "GAL3EC2" -> 90,
      "GAL3EC3" -> 30,
      "GAL3EC4" -> 30,
      "GAL3RU" -> 90,
      "GAPAYTM" -> 90,
      "SIGNAL" -> 90
    )
    levelMap.get(level)
  }

  //
  def isGA(level: String): Boolean = {
    level.startsWith("GA") && level != "GAMEPIND" && level != "GATravel"
  }

  val level2Schema = Map(
    "BANK" -> BankSchema,
    "PAYTM" -> L1Schema,
    "L2" -> L2Schema,
    "L3BK" -> L3BKSchema,
    "L3EC1" -> L3EC1Schema,
    "L3EC2" -> L3EC2Schema,
    "L3EC3" -> L3EC3Schema,
    "L3EC4" -> L3EC4Schema,
    "L3RU" -> L3RUSchema,
    "PAYMENTS" -> PaymentsSchema,
    "ONLINE" -> OnlineSchema,
    "WALLET" -> WalletSchema,
    "LMS" -> LMSSchema,
    "LMSV2" -> LMSV2Schema,
    "GABK" -> GABKSchema,
    "GAPAYTM" -> GAL1Schema,
    "GAL2" -> GAL2Schema,
    "GAL2RU" -> GAL2RUSchema,
    "GAL3RU" -> GAL3RUSchema,
    "GAL3BK" -> GAL3BKSchema,
    "GAL3EC1" -> (GAL3EC1Schema ++ GAL3EC1LPSchema).distinct,
    "GAL3EC2" -> (GAL3EC2Schema ++ GAL3EC2LPSchema).distinct,
    "GAL3EC3" -> (GAL3EC3Schema ++ GAL3EC3LPSchema).distinct,
    "GAL3EC4" -> (GAL3EC4Schema ++ GAL3EC4LPSchema).distinct
  )
}
