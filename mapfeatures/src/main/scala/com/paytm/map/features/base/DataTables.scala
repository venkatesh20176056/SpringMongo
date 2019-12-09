package com.paytm.map.features.base

import java.sql.Timestamp

import com.paytm.map.features.base.BaseTableUtils.{dayIterator, _}
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.base.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{min, to_date, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, _}
import org.joda.time.{DateTime, LocalTime}

import scala.util.{Failure, Success, Try}

case class BillDueDateRecord(
  customer_id: Long,
  operator: String,
  due_date: String,
  bill_fetch_date: String,
  service: String,
  paytype: String,
  bill_amount: Double,
  created_at: Timestamp,
  updated_at: Timestamp
)

case class LMSContactability(
  customer_id: String,
  sms: Int,
  ivr: Int,
  telecalling: Int,
  updated_at: String
)

case class LMSContactabilityV2(
  user_id: Option[String],
  sms: Option[Int],
  ivr: Option[Int],
  telecalling: Option[Int],
  updated_at: String
)

object DataTables {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // Read Tables
  def promocodeTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val rowSchema = StructType(Array(
      StructField("customer_id", LongType, true),
      StructField("redemption_type", StringType, true),
      StructField("order_item_id", LongType, true),
      StructField("amount", DoubleType, true),
      StructField("status", IntegerType, true),
      StructField("dt", DateType, true),
      StructField("campaign", StringType, true)
    ))

    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(rowSchema)

    readTable(spark, pathList, "code", "promocode_usage_snapshot_v2", targetDate, startDate, isV2)
      .select(
        $"user_id".cast(LongType).as("customer_id"),
        $"redemption_type",
        $"order_item_id".cast(LongType).as("order_item_id"),
        ($"amount" / 1000).as("amount"),
        $"status",
        to_date(substring($"created_at", 0, 10)).as("dt"),
        $"campaign"
      )
      .where($"dt" >= startDate)
      .where($"customer_id".isNotNull)
      .where($"order_item_id".isNotNull)
      .where($"status" === 1)
      .groupByKey((r: Row) => (r.getAs[Long]("order_item_id"), r.getAs[String]("campaign")))
      .mapGroups { (_, group) =>
        val groupList = group.toList
        val cashback = groupList.find((r: Row) => r.getAs[String]("redemption_type") == "cashback")
        cashback match {
          case Some(cbRow) => cbRow
          case _           => groupList.head
        }
      }
  }

  def allPromoUsagesTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val toPromoDate = udf[PromoUsageBase, String, String] {
      (promo, date) => PromoUsageBase(promo, date)
    }

    promocodeTable(spark, pathList, targetDate, startDate, isV2)
      .withColumn("promo_date", toPromoDate($"campaign", $"dt"))
      .drop("redemption_type")
  }

  def cashbackPromoUsagesTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    promocodeTable(spark, pathList, targetDate, startDate, isV2)
      .filter($"redemption_type" === "cashback")
      .drop("redemption_type")
      .withColumn("isPromoCodeJoin", lit(true))
  }

  def getCampaigns(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    readTableV3(spark, pathList) // always read V3 withour any date filter as its collated data
  }

  def getPromos(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    readTable(spark, pathList, "promocard", "promo_supercash", targetDate, startDate, isV2)
  }

  def metaDataTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    val passengersType = ArrayType(StructType(Seq(
      StructField("primary", StringType, nullable = true),
      StructField("total_fare", StringType, nullable = true),
      StructField("ticket_order_fare", StringType, nullable = true),
      StructField("conv_fee", StringType, nullable = true),
      StructField("ticket_id", StringType, nullable = true),
      StructField("ticket_uid", StringType, nullable = true),
      StructField("age", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("idName", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("mobile_number", StringType, nullable = true),
      StructField("seat_number", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("trip_id", StringType, nullable = true),
      StructField("boarding_point_id", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("destination", StringType, nullable = true),
      StructField("travel_date", StringType, nullable = true),
      StructField("departure_time", StringType, nullable = true),
      StructField("boarding_date", StringType, nullable = true),
      StructField("boarding_time", StringType, nullable = true),
      StructField("is_ac", StringType, nullable = true),
      StructField("operator_name", StringType, nullable = true)
    )), containsNull = true)

    val metaSchema = StructType(Seq(
      StructField("service_operator", StringType, nullable = true),
      StructField("language", StringType, nullable = true),
      StructField("providerName", StringType, nullable = true),
      StructField("movieCode", StringType, nullable = true),
      StructField("entityId", StringType, nullable = true),
      StructField("trip_type", StringType, nullable = true),
      StructField("booking_type", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("destination", StringType, nullable = true),
      StructField("travel_date", StringType, nullable = true),
      StructField("infants", IntegerType, nullable = true),
      StructField("class", StringType, nullable = true),
      StructField("distance", StringType, nullable = true),
      StructField("passengers", passengersType)

    ))

    import spark.implicits._
    readTable(spark, pathList, "marketplace", "sales_order_metadata_snapshot_v2", targetDate, startDate, isV2)
      .withColumn("jsonData", from_json($"meta_data", metaSchema))
      .withColumn("flight_operator", $"jsonData"("service_operator"))
      .withColumn("language", $"jsonData"("language"))
      .withColumn("service_operator", $"jsonData"("providerName"))
      .withColumn("movie_code", $"jsonData"("movieCode"))
      .withColumn("event_entity_id", $"jsonData"("entityId"))
      .withColumn("flight_type", $"jsonData"("trip_type"))
      .withColumn("booking_type", $"jsonData"("booking_type"))
      .withColumn("source", $"jsonData"("source"))
      .withColumn("destination", $"jsonData"("destination"))
      .withColumn("travel_date", $"jsonData"("travel_date"))
      .withColumn("infant", $"jsonData"("infants"))
      .withColumn("class", $"jsonData"("class"))
      .withColumn("distance", $"jsonData"("distance"))
      .withColumn("boarding_point_id", $"jsonData"("passengers")(0)("boarding_point_id"))
      .withColumn("order_item_id", $"order_item_id".cast(LongType))
      .select(
        "order_item_id",
        "flight_operator",
        "language",
        "service_operator",
        "movie_code",
        "event_entity_id",
        "flight_type",
        "booking_type",
        "source",
        "destination",
        "travel_date",
        "infant",
        "class",
        "distance",
        "boarding_point_id"

      )
      .withColumn("isMetaDataJoin", lit(true))
  }
  // This function now returns unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are calling this) the filter .where(col("successfulTxnFlag") === 1)
  def soiTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._
    readTable(spark, pathList, "marketplace", "sales_order_item_snapshot_v2", targetDate, startDate, isV2)
      .select(
        $"id".cast(LongType).as("order_item_id"),
        $"order_id".cast(LongType).as("order_id"),
        $"status",
        $"name",
        $"product_id",
        $"merchant_id".as("dmid"), //Rename it when needed, for now its being used for fees features
        ($"selling_price" / 1000).as("selling_price"),
        $"vertical_id".cast(IntegerType).as("vertical_id"),
        from_unixtime(unix_timestamp($"created_at")).as("created_at"),
        ((($"price" * $"qty_ordered") - $"selling_price") / 1000).as("discount"),
        to_date(substring($"created_at", 0, 10)).as("dt"),
        to_date(substring($"updated_at", 0, 10)).as("updated_at"),
        $"fulfillment_req"
      )
      .where($"dt" >= startDate)
      .where($"order_item_id".isNotNull)
      .where($"order_id".isNotNull)
      .where($"created_at".isNotNull)
      .withColumn("discount", when($"discount" < 0, 0).otherwise($"discount"))
      .withColumn("successfulTxnFlag", successfulTxnFlag)
      .withColumn("successfulTxnFlagEcommerce", successfulTxnFlagEcommerce)
  }

  def soTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    readTable(spark, pathList, "marketplace", "sales_order_snapshot_v2", targetDate, startDate, isV2)
      .select(
        $"id".cast(LongType).as("order_id"),
        $"customer_id".cast(LongType).as("customer_id"),
        $"channel_id",
        $"phone",
        $"payment_status"
      )
      .where($"customer_id".isNotNull) // Adding safety for the type casted var
      .where($"order_id".isNotNull)
  }

  def getSalesTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    if (isV2) {
      // For backfill
      val so = soTable(spark, Map(), targetDate, startDate, isV2)
      val soi = soiTable(spark, Map(), targetDate, startDate, isV2)
      soi.join(so, "order_id")
    } else {
      readTableV3(spark, pathList, targetDate, startDate)
        .select(
          $"order_item_id",
          $"order_id",
          $"customer_id".cast(LongType).as("customer_id"),
          $"status",
          $"name",
          $"product_id",
          $"merchant_id".as("dmid"), //Rename it when needed, for now its being used for fees features
          ($"selling_price" / 1000).as("selling_price"),
          $"vertical_id".cast(IntegerType).as("vertical_id"),
          from_unixtime(unix_timestamp($"created_at")).as("created_at"),
          ((($"price" * $"qty_ordered") - $"selling_price") / 1000).as("discount"),
          to_date(substring($"created_at", 0, 10)).as("dt"),
          to_date(substring($"updated_at", 0, 10)).as("updated_at"),
          $"channel_id",
          $"phone"
        )
        .where($"dt" >= startDate)
        .where($"order_item_id".isNotNull)
        .where($"order_id".isNotNull)
        .where($"created_at".isNotNull)
        .withColumn("discount", when($"discount" < 0, 0).otherwise($"discount"))
        .withColumn("successfulTxnFlag", successfulTxnFlag)
        .withColumn("successfulTxnFlagEcommerce", successfulTxnFlagEcommerce)
        .where($"customer_id".isNotNull) // Adding safety for the type casted var
        .where($"order_id".isNotNull)
    }
  }

  def sopTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    readTable(spark, pathList, "marketplace", "sales_order_payment_snapshot_v2", targetDate, startDate, isV2)
      .where($"order_id".isNotNull)
      .groupBy($"order_id".cast(LongType).as("order_id"))
      .agg(
        collect_list("payment_bank") as "payment_bank",
        collect_list("payment_method") as "payment_method"
      )
      .withColumn("isSOPJoin", lit(true))
  }

  def catalogCategoryTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"business_vertical".cast(IntegerType).as("business_vertical_id"),
        $"id".cast(IntegerType).as("category_id"),
        $"vertical_id",
        $"name"
      )
      .where($"category_id".isNotNull && $"business_vertical_id".isNotNull)
  }

  def catalogCategoryGLPTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"id".cast(IntegerType).as("glpID"),
        $"parent_id".cast(IntegerType).as("category_id"),
        $"name",
        $"path",
        $"type",
        $"status"
      )
      .where($"glpID".isNotNull)
      .where($"category_id".isNotNull)
      .where($"type".isin(1, 2))
      .where($"status" === 1)
  }

  def catalogEntityDecoratorTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"id".cast(IntegerType).as("clpID"),
        $"entity_associated_with".cast(IntegerType).as("category_id"),
        $"name",
        $"status"
      )
      .where($"clpID".isNotNull)
      .where($"category_id".isNotNull)
      .where($"status" === 1)
  }

  def rechargeTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList) // static table
      .select(
        lower($"service").as("service"),
        $"paytype",
        $"operator",
        $"product_id".cast(StringType).as("product_id")
      )
      .withColumn("isRechargeJoin", lit(true))
  }

  def rechargeTableWithNum(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    readTable(spark, pathList, "fs_recharge", "fulfillment_recharge_snapshot_v2", targetDate, startDate, isV2)
      .select(
        $"item_id".cast(LongType).as("order_item_id"),
        lower($"service").as("service"),
        $"paytype",
        $"operator",
        $"product_id".cast(StringType).as("product_id"),
        $"info",
        $"recharge_number_1",
        $"recharge_number_2",
        $"recharge_number_3",
        $"recharge_number_4",
        $"amount",
        $"circle",
        $"updated_at".as("updated_at_rech")
      )
      .withColumn("isRechargeJoin", lit(true))
  }

  // This function now returns unfiltered data(for last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are calling this) the filter .where(col("successfulTxnFlag") === 1)
  def newSTRTable(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    readTable(spark, pathList, "wallet", "new_system_txn_request_snapshot_v2", targetDate, startDate, isV2)
      .select(
        from_unixtime(unix_timestamp($"create_timestamp")).as("created_at"),
        to_date(substring($"create_timestamp", 0, 10)).as("dt"),
        $"id".as("str_id"),
        $"payer_id".cast(LongType).as("customer_id"),
        $"payee_id".cast(LongType),
        $"txn_amount",
        $"txn_status",
        $"txn_type",
        $"payee_wallet_id".cast(LongType).as("wallet_id"),
        $"payer_type",
        $"merchant_order_id",
        $"alipay_trans_id",
        lower(get_json_object($"response_metadata", "$.paymentMode")) as "payment_method",
        to_date(substring($"update_timestamp", 0, 10)).as("updated_date"),
        $"mode",
        $"metadata"
      )
      .where($"payer_id".isNotNull)
      .where($"wallet_id".isNotNull)
      .where($"dt" >= startDate)
      .withColumn("isnewSTRJoin", lit(true))
      .withColumn("isAddCash", when($"txn_type".isin(36), 1).otherwise(0))
      .withColumn("isAddMoney", when($"txn_type".isin(20, 4), 1).otherwise(0))
      .withColumn("isP2P", when($"txn_type".isin(5, 69), 1).otherwise(0))
      .withColumn("isP2B", when($"txn_type".isin(29), 1).otherwise(0))
      .withColumn("isUserToMerchant", when($"txn_type".isin(1) && $"payer_type" === 3, 1).otherwise(0))
      .withColumn("byCC", when($"payment_method" === "cc", 1).otherwise(0))
      .withColumn("byDC", when($"payment_method" === "dc", 1).otherwise(0))
      .withColumn("byNB", when($"payment_method" === "nb", 1).otherwise(0))
      .withColumn("byNEFT", when($"payment_method" === "neft", 1).otherwise(0))
      .withColumn("byUPI", when($"payment_method" === "upi", 1).otherwise(0))
      .withColumn("successfulTxnFlag", when($"txn_status" === 1, 1).otherwise(0))
  }

  def corporateMerchantsTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"mid".cast(LongType)
      )
  }

  def ppiDetailsTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"id".cast(LongType),
        $"balance".cast(DoubleType),
        $"ppi_type",
        $"merchant_id".cast(LongType),
        $"parent_wallet_id",
        to_date(substring($"create_timestamp", 0, 10)).as("dt"),
        $"create_timestamp"
      )
  }

  def getOnPaytmMID(spark: SparkSession, settings: Settings, targetDateStr: String): Array[String] = {
    import spark.implicits._
    val ed = readTable(spark, settings.datalakeDfs.entityDemographics, s"pg", s"entity_demographics", targetDateStr).filter($"status" === 9376503)
    val ei = readTable(spark, settings.datalakeDfs.entityInfo, s"pg", s"entity_info", targetDateStr).filter($"status" === 9376503)
    val merchant = readTable(spark, settings.datalakeDfs.merchantUser, s"wallet", s"merchant", targetDateStr)

    val exclusionList = ed.join(ei, ed("entity_id") === ei("id")).filter($"category" === "OnPaytm").select($"mid").distinct
    val OnUsMerchantId = merchant.join(exclusionList, merchant("pg_mid") === exclusionList("mid"), "inner").select("id").distinct().rdd.map(r => r(0).toString).collect()
    OnUsMerchantId
  }

  def getSalesAndWallet(sales: DataFrame, wallet: DataFrame): DataFrame = {
    val walletAligned = wallet.alignSchema(sales.schema)
    val intersectionDF = sales.join(walletAligned, Seq("order_id"), "inner")
    val unionDF = sales.union(walletAligned)
    val salesAndWalletTable = unionDF.join(intersectionDF, Seq("order_item_id"), "left_anti")
    salesAndWalletTable
  }

  def merchantTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"display_name".as("operator"),
        $"wallet_id".cast(LongType).as("wallet_id"),
        lower(trim($"category")).as("category"),
        lower(trim($"sub_category")).as("sub_category"),
        $"id".as("merchant_id"),
        $"name".as("mid"),
        $"pg_mid",
        to_date($"create_timestamp").as("signup_date")
      )
      .where($"wallet_id".isNotNull)
      .withColumn("isMerchantTableJoin", lit(true))
      .withColumn("isOnPaytm", when(lower($"operator").like("%paytm%"), 1).otherwise(0))
  }

  def catalogProductTable(spark: SparkSession, pathList: Map[String, String], isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList) // always read from V3 version as its collated data.
      .select(
        $"category_id",
        $"id".as("product_id"),
        $"brand_id",
        $"vertical_id"
      ).distinct()
      .withColumn("isCatalogProductJoin", lit(true))
  }

  def walletBankTxn(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String): DataFrame = {
    readTableV3(spark, pathList, targetDate, startDate)
  }

  ////////////// Gold Tables ///////////////////////////////////////////////////////////

  def goldBuyOrders(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList, targetDate, startDate)
      .filter($"customer_id" =!= "429215822")
      .select(
        to_date($"created_at").alias("dt"),
        to_date($"created_at").alias("created_at"),
        $"order_item_id",
        $"order_id",
        $"amount".alias("selling_price"),
        $"gram_weight",
        $"customer_id_text".alias("customer_id"),
        $"status"
      )
  }

  def goldSellOrders(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList, targetDate, startDate)
      .select(
        to_date($"created_at").alias("dt"),
        to_date($"created_at").alias("created_at"),
        $"order_item_id",
        $"order_id",
        $"amount".alias("selling_price"),
        $"gram_weight",
        $"customer_id_text".alias("customer_id"),
        $"status"
      )

  }

  def goldBackOrders(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList, targetDate, startDate)
      .filter($"status" === 7)
      .filter($"customer_id" === "429215822")
      .select(
        to_date($"created_at").alias("dt"),
        to_date($"created_at").alias("created_at"),
        $"amount",
        $"customer_id_text" as "customer_id"
      )

  }

  def goldWallet(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList, targetDate, startDate)
      .select(
        $"customer_id",
        $"gold_balance"
      )
  }

  def upiVirtualAddressTable(spark: SparkSession, oldPathList: Map[String, String], newPathList: Map[String, String], isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val oldTable = readTableV3(spark, oldPathList)
      .filter($"status".equalTo("A"))
      .select(
        $"user_id",
        $"create_date" as "created_on"
      )

    val newTable = readTableV3(spark, newPathList)
      .filter($"status".equalTo("ACTIVE"))
      .select(
        $"user_id",
        $"created_on"
      )

    oldTable.union(newTable)
  }

  def upiBankAccountsTable(spark: SparkSession, oldPathList: Map[String, String], newPathList: Map[String, String], isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val oldTable = readTableV3(spark, oldPathList)
      .filter($"status".equalTo("A"))
      .select(
        $"user_id",
        $"create_date" as "created_on",
        when($"mbeba_flag".equalTo("Y"), 1).otherwise(0) as "mpin_set"
      )

    val newTable = readTableV3(spark, newPathList)
      .filter($"status".equalTo("ACTIVE"))
      .select(
        $"user_id",
        $"created_on",
        $"mpin_set"
      )

    oldTable.union(newTable)
  }

  def upiUserTable(spark: SparkSession, oldPathList: Map[String, String], newPathList: Map[String, String], isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val oldTable = readTableV3(spark, oldPathList)
      .select(
        $"id".as("user_id"),
        $"token_customer_id" as "customer_id"
      )

    val newTable = readTableV3(spark, newPathList)
      .select(
        $"id".as("user_id"),
        $"scope_cust_id" as "customer_id"
      )

    oldTable.union(newTable).where($"customer_id".isNotNull).dropDuplicates
  }

  def upiDeviceBindingTable(spark: SparkSession, oldPathList: Map[String, String], newPathList: Map[String, String], isV2: Boolean = false): DataFrame = {
    import spark.implicits._

    val oldTable = readTableV3(spark, oldPathList)
      .filter($"status".equalTo("A"))
      .select(
        $"user_id",
        $"created_date" as "created_on"
      )

    val newTable = readTableV3(spark, newPathList)
      .filter($"status".equalTo("ACTIVE"))
      .select(
        $"user_id",
        $"created_on"
      )

    oldTable.union(newTable)
  }

  ///////////// Bill Due Dates Tables /////////////////////////////////////////////////

  def billDueDateTable(spark: SparkSession, billsPathList: Array[String], prepaidPathList: Array[String]): DataFrame = {
    import spark.implicits._

    val prepaidExpiriesDF = spark.read.parquet(prepaidPathList: _*)
      .select(
        $"customer_id",
        $"validity_expiry_date" as "due_date",
        $"order_date" as "bill_fetch_date",
        lower($"operator") as "operator",
        lower($"service") as "service",
        lit("prepaid") as "paytype",
        $"amount".cast(DoubleType) as "bill_amount",
        $"created_at", $"updated_at"
      )

    val otherBillsDF = spark.read.parquet(billsPathList: _*)
      .select($"customer_id", $"due_date", $"bill_fetch_date",
        lower($"operator") as "operator",
        lower($"service") as "service",
        lower($"paytype") as "paytype",
        $"amount".cast(DoubleType) as "bill_amount",
        $"created_at", $"updated_at")

    val prepaidDueDates = getMaxDueDate(spark, prepaidExpiriesDF)
    val postpaidDueDates = getMinDueDate(spark, otherBillsDF)

    prepaidDueDates.union(postpaidDueDates)

  }

  def getMaxDueDate(spark: SparkSession, billsDF: DataFrame): DataFrame =
    getDueDate(spark, billsDF, _ > _)
  def getMinDueDate(spark: SparkSession, billsDF: DataFrame): DataFrame =
    getDueDate(spark, billsDF, _ < _)

  def getDueDate(
    spark: SparkSession,
    billsDF: DataFrame,
    op: (String, String) => Boolean
  ): DataFrame = {
    import spark.implicits._

    def toOperatorDueDate = udf[OperatorDueDate, String, Timestamp, Double](
      (opt: String, due: Timestamp, amt: Double) => OperatorDueDate(opt, due, amt)
    )

    billsDF
      .filter(col("updated_at").isNotNull)
      .filter(col("due_date").isNotNull) //Bills with no due dates aren't useful
      .filter(col("amount") > 0) // Keep just unpaid bills
      .as[BillDueDateRecord]
      .groupByKey(r => (r.customer_id, r.operator, r.paytype)) // Dedupe operator and paytype for each customer
      .reduceGroups((x, y) => if (op(x.due_date, y.due_date)) x else y) // and get either max or min due date
      .map(_._2) // keep the row and not key
      .groupByKey(r => (r.customer_id, r.operator, r.paytype, r.due_date)) // Dedupe due dates for each operator and paytype
      .reduceGroups((x, y) => if (x.updated_at.after(y.updated_at)) x else y) // and get latest updated time
      .map(_._2) // keep the row and not key
      .withColumn(
        "operator_due_dates",
        toOperatorDueDate(col("operator"), col("due_date").cast(TimestampType), col("bill_amount"))
      )
      .drop("due_date")
      .drop("bill_amount")
  }
  /////////////////////////////////////////////////////////////////////////////////////

  // SMS Data

  def SMSTable(
    spark: SparkSession,
    smsPurchasesPaths: Array[String],
    smsBillsPaths: Array[String],
    smsTravelPaths: Array[String]
  ): DataFrame = {
    import com.paytm.map.features.utils.ConvenientFrame._
    import spark.implicits._

    val userBillsDF = spark.read.parquet(smsBillsPaths: _*)
      .select(
        $"userId" as "customer_id",
        $"billName" as "sender_name",
        $"dateCreated" as "sent_timestamp"
      ).na.drop

    val travelDF = spark.read.parquet(smsTravelPaths: _*)
      .select(
        $"userId" as "customer_id",
        $"merchantName" as "sender_name",
        $"dateCreated" as "sent_timestamp"
      ).na.drop

    val purchasesDF = spark.read.parquet(smsPurchasesPaths: _*)
      .select(
        $"userId" as "customer_id",
        $"merchantName" as "sender_name",
        $"dateCreated" as "sent_timestamp"
      ).na.drop

    Seq(userBillsDF, travelDF, purchasesDF)
      .unionAll
      .withColumn("sent_timestamp", unix_timestamp($"sent_timestamp", "dd/MM/yyyy HH:mm:ss").cast("timestamp"))
      .withColumn("sender_name_and_time", struct($"sender_name", $"sent_timestamp"))
      .drop("sender_name")
      .drop("sent_timestamp")
  }

  //////////////////////////////////////////////////////////////////////////////////////

  // use this for app
  def gaTableApp(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    spark.read.parquet(path)
      .select(
        $"app_current_name",
        $"customer_id".cast(LongType).as("customer_id"),
        $"product_sku" as "product_id",
        $"product_list_name",
        $"event_action",
        $"session_id",
        $"hit_type",
        $"date"
      )
      .where($"customer_id".isNotNull)
      .where($"product_id".isNotNull)
      .where($"app_current_name".isNotNull && ($"app_current_name" =!= ""))
      .where($"event_action".isin("Product Impression", "Product Click", "Product Detail", null))
      .withColumn("created_date", split($"date", " ")(0)).drop($"date")
      .withColumn("session_identifier", concat($"session_id", $"app_current_name", $"product_id"))
      .drop("session_id", "product_list_name", "google_identity", "event_category", "date")
  }

  // Use this for web due to a different semantics of app_current_name
  // DAAS told that page_path is used instead of app_current_name
  // has been verified with data
  def gaTableWeb(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    spark.read.parquet(path)
      .select(
        $"page_path" as "app_current_name",
        $"customer_id".cast(LongType).as("customer_id"),
        $"product_sku" as "product_id",
        $"product_list_name",
        $"event_action",
        $"session_id",
        $"hit_type",
        $"date"
      )
      .where($"customer_id".isNotNull)
      .where($"product_id".isNotNull)
      .where($"app_current_name".isNotNull && ($"app_current_name" =!= ""))
      .where($"event_action".isin("Product Impression", "Product Click", "Product Detail", null))
      .withColumn("created_date", split($"date", " ")(0))
      .withColumn("session_identifier", concat($"session_id", $"app_current_name", $"product_id"))
      .drop("session_id", "product_list_name", "google_identity", "event_category", "date")
  }

  // use this for app raw events
  // all event_actions are returned
  def gaTableAppRaw(spark: SparkSession, path: String, filterCondition: Column): DataFrame = {
    import spark.implicits._
    spark.read.parquet(path)
      .select(
        $"app_current_name",
        $"customer_id".cast(LongType).as("customer_id"),
        $"product_sku" as "product_id",
        $"product_list_name",
        $"event_action",
        $"event_category",
        $"session_id",
        $"date"
      )
      .where($"customer_id".isNotNull)
      .where($"app_current_name".isNotNull && ($"app_current_name" =!= ""))
      .withColumn("created_date", split($"date", " ")(0)).drop($"date")
      .withColumn("session_identifier", concat($"session_id", $"app_current_name"))
      .drop("session_id", "product_list_name", "google_identity", "date")
      .filter(filterCondition)
  }

  // Use this for web raw events
  // all event_actions are returned
  def gaTableWebRaw(spark: SparkSession, path: String, filterCondition: Column): DataFrame = {
    import spark.implicits._
    spark.read.parquet(path)
      .select(
        $"page_path" as "app_current_name",
        $"customer_id".cast(LongType).as("customer_id"),
        $"product_sku" as "product_id",
        $"product_list_name",
        $"event_action",
        $"event_category",
        $"session_id",
        $"date"
      )
      .where($"customer_id".isNotNull)
      .where($"app_current_name".isNotNull && ($"app_current_name" =!= ""))
      .withColumn("created_date", split($"date", " ")(0))
      .withColumn("session_identifier", concat($"session_id", $"app_current_name"))
      .drop("session_id", "product_list_name", "google_identity", "date")
      .filter(filterCondition)
  }

  def gaJoinedWebApp(
    spark: SparkSession,
    gaAppRootPaths: Map[String, String],
    gaWebRootPaths: Map[String, String],
    gaMallAppRootPaths: Map[String, String],
    gaMallWebRootPaths: Map[String, String],
    targetDate: DateTime,
    startDate: DateTime
  ): DataFrame = {

    val dtSeq = dayIterator(startDate, targetDate, ArgsUtils.formatter)

    dtSeq.map(reqDate => {
      println("Reading GA DATA FROM: " + reqDate)
      try {
        val reqDateKey = s"created_at=" + reqDate

        val gaAPPPath = gaAppRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaWebPath = gaWebRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaMallAPPPath = gaMallAppRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaMallWebPath = gaMallWebRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        Seq(
          gaTableApp(spark, gaAPPPath).withColumn("isApp", lit(1)).withColumn("dt", lit(reqDate)).withColumn("isMall", lit(0)),
          gaTableApp(spark, gaMallAPPPath).withColumn("isApp", lit(1)).withColumn("dt", lit(reqDate)).withColumn("isMall", lit(1)),
          gaTableWeb(spark, gaWebPath).withColumn("isApp", lit(0)).withColumn("dt", lit(reqDate)).withColumn("isMall", lit(0)),
          gaTableWeb(spark, gaMallWebPath).withColumn("isApp", lit(0)).withColumn("dt", lit(reqDate)).withColumn("isMall", lit(1))
        ).reduce(_ union _)

      } catch {
        case e: Throwable =>
          println(s"Caught Exception while reading GA data and skipping execution for $reqDate. Exception: ${e.getMessage}")

          val GABaseSchema = StructType(
            StructField("app_current_name", StringType, nullable = true) ::
              StructField("customer_id", LongType, nullable = true) ::
              StructField("product_id", StringType, nullable = true) ::
              StructField("event_action", StringType, nullable = true) ::
              StructField("hit_type", StringType, nullable = true) ::
              StructField("created_date", StringType, nullable = true) ::
              StructField("session_identifier", StringType, nullable = true) ::
              StructField("isApp", LongType, nullable = true) ::
              StructField("isMall", LongType, nullable = true) ::
              StructField("dt", StringType, nullable = true) :: Nil
          )

          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], GABaseSchema)
      }
    }).reduce(_ union _)

  }

  def gaJoinedWebAppRaw(
    spark: SparkSession,
    gaAppRootPaths: Map[String, String],
    gaWebRootPaths: Map[String, String],
    gaMallAppRootPaths: Map[String, String],
    gaMallWebRootPaths: Map[String, String],
    targetDate: DateTime,
    startDate: DateTime,
    filterCondition: Column = lit(true)
  ): DataFrame = {

    val dtSeq = dayIterator(startDate, targetDate, ArgsUtils.formatter)

    dtSeq.map(reqDate => {
      println("Reading GA RAW DATA FROM: " + reqDate)
      try {
        val reqDateKey = s"created_at=" + reqDate

        val gaAPPPath = gaAppRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaWebPath = gaWebRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaMallAPPPath = gaMallAppRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")
        val gaMallWebPath = gaMallWebRootPaths.get(reqDateKey).last.replace("s3:", "s3a:")

        Seq(
          gaTableAppRaw(spark, gaAPPPath, filterCondition)
            .withColumn("isApp", lit(1))
            .withColumn("dt", lit(reqDate))
            .withColumn("isMall", lit(0)),
          gaTableAppRaw(spark, gaMallAPPPath, filterCondition)
            .withColumn("isApp", lit(1))
            .withColumn("dt", lit(reqDate))
            .withColumn("isMall", lit(1)),
          gaTableWebRaw(spark, gaWebPath, filterCondition)
            .withColumn("isApp", lit(0))
            .withColumn("dt", lit(reqDate))
            .withColumn("isMall", lit(0)),
          gaTableWebRaw(spark, gaMallWebPath, filterCondition)
            .withColumn("isApp", lit(0))
            .withColumn("dt", lit(reqDate))
            .withColumn("isMall", lit(1))
        ).reduce(_ union _)
      } catch {
        case e: Throwable =>
          println(s"Caught Exception while reading GA data and skipping execution for $reqDate. Exception: ${e.getMessage}")

          val GABaseSchema = StructType(
            StructField("app_current_name", StringType, nullable = true) ::
              StructField("customer_id", LongType, nullable = true) ::
              StructField("product_id", StringType, nullable = true) ::
              StructField("event_action", StringType, nullable = true) ::
              StructField("event_category", StringType, nullable = true) ::
              StructField("created_date", StringType, nullable = true) ::
              StructField("session_identifier", StringType, nullable = true) ::
              StructField("isApp", LongType, nullable = true) ::
              StructField("isMall", StringType, nullable = true) ::
              StructField("dt", StringType, nullable = true) :: Nil
          )

          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], GABaseSchema)
      }
    }).reduce(_ union _)

  }

  def offlineOrganizedMerchant(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    val unOrgOffList = Seq("UnOrganized Offline", "Unorganized Offline", "Unorganized offline")
    val KAMList = Seq("KAM", "KEY Account", "Key Accounts", "Key Accounts_Hub", "Key Accounts_Mall")
    val merchantL2Category = when(trim($"agent_type").isin(unOrgOffList: _*), "UNORG")
      .when(trim($"agent_type").isin(KAMList: _*), "KAM")
      .when(trim($"agent_type") === "DIY", "DIY")
      .otherwise("OTHERS")

    readTableV3(spark, pathList)
      .select(
        $"pg_entity_id".as("entity_id"),
        $"pg_mid".as("mid"),
        lit(1).as("isMerchant"),
        $"pg_aggregator_merchant_name".as("merchant_first_name"),
        $"pg_aggregator_merchant_name".as("merchant_last_name"),
        lit(null).cast(StringType).as("merchant_email_ID"),
        lit("OFFLINE").as("merchant_L1_type"),
        merchantL2Category.as("merchant_L2_type"),
        $"pg_category".as("merchant_category"),
        $"pg_sub_category".as("merchant_subcategory"),
        $"w_merchant_id".as("merchant_id")
      )

  }

  def UIDEIDMapper(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    val localTime = (new LocalTime()).toString
    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"eid".as("entity_id"),
        $"uid".as("customer_id"),
        $"created_on",
        lead($"created_on", 1, localTime).over(Window.partitionBy($"eid").orderBy($"created_on".asc)).as("expire_on")
      )
      .withColumn("isActive", when($"expire_on" === localTime, 1))

  }

  def offlineRetailer(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._

    readTableV3(spark, pathList)
      .select(
        $"intcust_id".cast(LongType).as("customer_id"),
        lit(null).cast(StringType).as("mid"),
        lit(1).as("isMerchant"),
        $"merchant_name".as("merchant_first_name"),
        $"merchant_name".as("merchant_last_name"),
        lit(null).cast(StringType).as("merchant_email_ID"),
        lit("OFFLINE").as("merchant_L1_type"),
        lit("P2P").as("merchant_L2_type"),
        $"main_category".as("merchant_category"),
        lit(null).cast(StringType).as("merchant_subcategory"),
        lit(null).cast(LongType).as("merchant_id"),
        to_date($"signup").as("signup_date")
      )
  }

  def urbanAirshipData(spark: SparkSession, path: String, targetDate: String, startDate: String): DataFrame = {
    import spark.implicits._

    spark.read.parquet(path)
      .filter($"dt" between (startDate, targetDate))
      .withColumn("push_type", when($"type".isin("SEND", "RICH_DELIVERY"), "RECEIVED")
        .otherwise(when($"type".isin("OPEN", "RICH_READ") ||
          ($"type" === "IN_APP_MESSAGE_RESOLUTION" && $"body_type" === "MESSAGE_CLICK"), "INTERACTION")
          .otherwise(null)))
      .select(
        to_date($"occurred").alias("occurred_date"),
        $"dt",
        $"body_named_user".alias("customer_id"),
        $"campaign_id",
        $"body_type",
        $"push_type"
      )
  }

  def pushLogs(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String): DataFrame = {
    import spark.implicits._

    val table = Try(readTableV3(spark, pathList, targetDate, startDate)
      .select(
        $"customer_id",
        $"date_id".alias("occurred_date"),
        to_date($"updated_at").alias("dt")
      ).withColumn("push_type", lit("RECEIVED")))
    table match {
      case Success(pushLogs) => {
        pushLogs
      }
      case Failure(exception) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Array(
        StructField("customer_id", IntegerType, true),
        StructField("occurred_date", DateType, true),
        StructField("push_type", StringType, true),
        StructField("dt", DateType, true)
      )))
    }
  }

  def entityInfo(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"id".as("entity_id"),
        $"MID".as("mid"),
        lit(1).as("isMerchant"),
        $"merchant_name".as("merchant_first_name"),
        $"merchant_name".as("merchant_last_name"),
        lit(null).cast(StringType).as("merchant_email_ID"),
        lit("ONLINE").as("merchant_L1_type"),
        lit(null).cast(StringType).as("merchant_L2_type")
      )

  }

  def entityDemographics(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"entity_id",
        $"CATEGORY".as("merchant_category"),
        $"SUB_CATEGORY".as("merchant_subcategory")
      )

  }

  def getPGAlipayOLAP(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {
    // customer_id can be null in alipay olap, add filter .where($"customer_id".isNotNull) if joining on customer_id
    import spark.implicits._
    readTable(spark, pathList, "dwh", "alipay_pg_olap", targetDate, startDate, isV2, partnCol = "ingest_date")
      .select(
        trim($"payment_mode") as "payment_mode",
        trim($"card_type") as "card_type",
        $"responsecode",
        $"card_issuer",
        $"card_category",
        $"txn_id".as("alipay_trans_id"),
        $"txn_status",
        $"cust_id".cast(LongType).as("customer_id"),
        $"mid",
        $"txn_amount",
        from_unixtime(unix_timestamp($"txn_started_at")).as("created_at"),
        to_date($"txn_started_at").as("dt"),
        $"payment_type",
        $"request_type"
      )
      .where($"dt" >= startDate)
  }

  def getPgTxnInfo(spark: SparkSession, settings: Settings, targetDateStr: String, startDateStr: String): DataFrame = {

    import spark.implicits._
    import settings._

    val pgTxnInfo = readTableV3(spark, datalakeDfs.pgTxnInfo, targetDateStr, startDateStr)
    val pgTxnMetaInfo = readTableV3(spark, datalakeDfs.pgTxnMetaInfo, targetDateStr, startDateStr)
    val pgTxnCardInfo = readTableV3(spark, datalakeDfs.pgTxnCardInfo, targetDateStr, startDateStr)
    val pgEntityInfo = readTableV3(spark, datalakeDfs.entityInfo)
    val pgLookup = readTableV3(spark, datalakeDfs.pgLookup)

    pgTxnInfo
      .join(pgEntityInfo, pgTxnInfo("entity_id") === pgEntityInfo("id"), "left_outer")
      .join(pgTxnMetaInfo, pgTxnInfo("txn_id") === pgTxnMetaInfo("txn_id"), "left_outer")
      .join(pgTxnCardInfo, pgTxnInfo("txn_id") === pgTxnCardInfo("txn_id"), "left_outer")
      .join(broadcast(pgLookup), pgTxnMetaInfo("payment_type_id") === pgLookup("lookup_id"), "left_outer")
      .select(
        pgTxnInfo("txn_id"),
        pgEntityInfo("mid"),
        pgTxnMetaInfo("cust_id") as "customer_id",
        pgTxnCardInfo("card_issuer"),
        trim(pgTxnCardInfo("card_type")) as "card_type",
        pgTxnCardInfo("card_category"),
        trim(pgLookup("name")) as "payment_mode",
        pgTxnInfo("responsecode"),
        to_date($"txn_started_at") as "dt"
      )
  }

  def entityPreferences(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"entity_id",
        $"pref_type",
        $"pref_value",
        $"status"
      )

  }

  def walletUser(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"cust_id".cast(LongType).as("customer_id"),
        $"sso_id".as("wallet_id"),
        $"wallet_rbi_type".as("wallet_type"),
        $"trust_factor",
        $"guid" // added new column for subwallet balance feature
      )
      .where($"customer_id".isNotNull)
      .where($"wallet_id".isNotNull)
  }

  def walletDetails(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"owner_id".as("wallet_id"),
        $"balance",
        $"id", // added new column for subwallet balance feature
        $"owner_guid" // added new column for subwallet balance feature
      )
      .where($"wallet_id".isNotNull)
  }

  def lmsAccount(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    val toLMSAccount = udf((account_number: String,
      status: Int,
      total_amount: Double,
      balance_amount: Double,
      due_date: Timestamp,
      late_fee: Double) =>
      LMSAccount(account_number, status, total_amount, balance_amount, due_date, late_fee))

    readTableV3(spark, pathList)
      .select(
        $"customer_id",
        struct($"created_at", $"updated_at", $"id", $"account_number", $"due_date", $"total_amount", $"balance_amount", $"late_fee", $"product_type", $"status") as "d"
      ).groupBy("customer_id").agg(max("d") as "d")
      .withColumn("account_details", toLMSAccount(
        $"d.account_number", $"d.status", $"d.total_amount",
        $"d.balance_amount", $"d.due_date".cast(DateType), $"d.late_fee"
      )).select($"customer_id", $"account_details", $"d.*")
  }

  def lmsContactability(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    // If more than one row for customer, read latest updated row
    readTableV3(spark, pathList)
      .select($"customer_id", $"sms", $"ivr", $"telecalling", $"updated_at")
      .as[LMSContactability]
      .groupByKey(r => r.customer_id)
      .reduceGroups((r1, r2) => if (r1.updated_at > r2.updated_at) r1 else r2)
      .map(_._2)
      .toDF("customer_id", "contact_by_SMS", "contact_by_IVR", "contact_by_teleCalling", "updated_at")
      .drop("updated_at")
  }

  def lmsAccountV2(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    readTableV3(spark, pathList)
  }

  def lmsCasesV2(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    readTableV3(spark, pathList)
      .select("case_id", "account_number", "product_type", "status")
      .where($"status" === 1)
  }

  def lmsContactabilityV2(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._
    // If more than one row for customer, read latest updated row
    readTableV3(spark, pathList)
      .select($"user_id", $"sms", $"ivr", $"telecalling", $"updated_at")
      .as[LMSContactabilityV2]
      .groupByKey(r => r.user_id)
      .reduceGroups((r1, r2) => if (r1.updated_at > r2.updated_at) r1 else r2)
      .map(_._2)
      .toDF("user_id", "contact_by_SMS", "contact_by_IVR", "contact_by_teleCalling", "updated_at")
      .drop("updated_at")
  }

  def offlineRFSE(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList).select($"fse_cust_id".cast(LongType).as("customer_id"))

  }

  def leadsTable(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .select("customer_id")
      .filter($"campaign_id" === 1002)
  }

  def digitalCreditAccounts(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .select(
        "customer_id",
        "lender",
        "application_status",
        "authorised_credit_limit",
        "due_amount",
        "account_number",
        "account_status",
        "extended_due_date",
        "available_credit_limit",
        "account_number",
        "due_date"
      )
      .filter($"lender" === 1)
  }

  def digitalCreditEligibleUsers(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    val temp = readTableV3Merge(spark, pathList)

    temp.printSchema

    temp
      .filter($"lender" === 1)
      .select($"customer_id", $"go_live_date", $"product_type")
  }

  def digitalCreditEarlyAccess(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .filter($"lender" === 1)
      .select($"customer_id")
  }

  def digitalCreditBills(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .select(
        $"created_at",
        to_date($"created_at").alias("dt"),
        $"account_number",
        $"total_amt"
      )
      .withColumn("month_year", date_format($"dt", "MM-yyyy"))
  }

  def digitalCreditStatement(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .filter($"type" === 0)
      .select($"account_number", $"updated_at")
  }

  def digitalCreditBankInitiated(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .filter($"type" === 3)
      .select(
        $"account_number",
        to_date($"updated_at").alias("dt"),
        $"amount"
      )
  }

  def digitalCreditRevolve(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .select($"account_number", to_date($"updated_at").alias("dt"))
  }

  def digitalCreditSubscriptions(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .filter($"status" === 2)
      .select($"account_number", $"status".alias("subscription_status"))
  }

  def digitalCreditEligibleUsersReporting(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .select(
        $"customer_id",
        $"is_micro_loan"
      )
  }

  def postPaidAcquisitionModel(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("date"))))
      .select(
        $"customer_id",
        $"score_decile"
      )
  }

  def postPaidAdoption(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("date"))))
      .filter($"rank" === 1)
      .select(
        $"customer_id",
        $"adoption_probability"
      )

  }

  def postPaidDeferEligible(spark: SparkSession, pathList: Map[String, String]): DataFrame = {
    import spark.implicits._

    readTableV3(spark, pathList)
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("date"))))
      .select(
        $"customer_id",
        lit(1).alias("defer_eligible")
      )

  }

  def flightsOLAP(spark: SparkSession, pathList: Map[String, String], startDate: String, targetDate: String): DataFrame = {
    import spark.implicits._
    // daas tables is partitioned by ingest_date, so to get all records created between startDate and targetDate, get all records ingested since startDate
    readTableV3Since(spark, pathList, startDate)
      .withColumn("dt", to_date($"created_at"))
      .withColumn("travel_date", to_date(from_utc_timestamp($"departuretime", "IST")))
      .where($"payment_status" === 2 && $"dt" >= startDate && $"dt" <= targetDate)
      .select(
        $"customer_id",
        $"order_id",
        $"travel_date",
        $"destination_city",
        $"destination_country",
        $"dt"
      )
  }

  def trainsOLAP(spark: SparkSession, pathList: Map[String, String], startDate: String, targetDate: String): DataFrame = {
    import spark.implicits._

    readTableV3Since(spark, pathList, startDate)
      .withColumn("dt", to_date($"created_at"))
      .withColumn("travel_date", to_date($"travel_date", "yyyyMMdd"))
      .where($"payment_status" === 2 && $"dt" >= startDate && $"dt" <= targetDate)
      .select(
        $"customer_id",
        $"order_id",
        $"travel_date",
        $"destination_name" as "destination_city",
        $"dt"
      )
  }

  def busOLAP(spark: SparkSession, pathList: Map[String, String], startDate: String, targetDate: String): DataFrame = {
    import spark.implicits._

    readTableV3Since(spark, pathList, startDate)
      .withColumn("dt", to_date($"created_at"))
      .withColumn("travel_date", $"departure_date")
      .where($"payment_status" === 2 && $"dt" >= startDate && $"dt" <= targetDate)
      .select(
        $"customer_id",
        $"order_id",
        $"travel_date",
        $"destination_city",
        $"operator",
        $"dt"
      )
  }

  /********************************************************************************************************************/
  // Bank Tables
  def physicalDebiCard(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"cust_id".cast(LongType).as("customer_id"),
        from_unixtime(unix_timestamp($"created_at")).as("created_at"),
        to_date(substring($"created_at", 0, 10)).as("dt"),
        to_date(substring($"updated_at", 0, 10)).as("updated_at")
      )
      .where($"customer_id".isNotNull)

  }

  def IDCDetails(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"cust_id".cast(LongType).as("customer_id")
      )
      .where($"customer_id".isNotNull)

  }

  def TBAADMGeneralAcctMast(spark: SparkSession, pathList: Map[String, String]): DataFrame = {

    import spark.implicits._
    readTableV3(spark, pathList)
      .select(
        $"cif_id".cast(LongType).as("customer_id"),
        $"acid",
        $"entity_cre_flg",
        $"del_flg",
        $"acct_cls_flg",
        $"acct_opn_date",
        $"schm_code",
        $"acct_ownership"
      )
      .where($"customer_id".isNotNull)
      .where($"acid".isNotNull)

  }

  def TBAADMHistTranDtl(spark: SparkSession, pathList: Map[String, String], targetDate: String, startDate: String, isV2: Boolean = false): DataFrame = {

    import spark.implicits._
    readTable(spark, pathList, "cbsprd", "tbaadm_hist_tran_dtl_table_snapshot_v3", targetDate, startDate, isV2)
      .select(
        $"acid",
        $"tran_id",
        $"pstd_flg",
        $"part_tran_type",
        $"rpt_code",
        $"tran_amt".cast(DoubleType).as("tran_amt"),
        from_unixtime(unix_timestamp($"tran_date")).as("created_at"),
        to_date(substring($"tran_date", 0, 10)).as("dt"),
        to_date(substring($"tran_date", 0, 10)).as("updated_at")
      )
      .where($"acid".isNotNull)
      .where($"dt" >= startDate)

  }

  /********************************************************************************************************************/
  // Derived Tables

  def pushCampaignTable(urbanAirshipData: DataFrame, pushLogs: DataFrame): DataFrame = {
    val pushLogTable = pushLogs
      .withColumn("campaign_id", lit(null))
      .withColumn("body_type", lit(null))
      .select(
        "customer_id",
        "dt",
        "push_type",
        "campaign_id",
        "occurred_date",
        "body_type"
      )

    val urbanAirshipTable = urbanAirshipData
      .select(
        "customer_id",
        "dt",
        "push_type",
        "campaign_id",
        "occurred_date",
        "body_type"
      )

    urbanAirshipTable.union(pushLogTable)
  }

  def entityExlusionList(entityDemographics: DataFrame, entityInfo: DataFrame): Dataset[Row] = {

    entityDemographics
      .where(col("entity_id").isNotNull)
      .where(lower(col("merchant_category")).isin("onpaytm", "test", "non transacting"))
      //.join(entityInfo,Seq("entity_id"),"inner")
      //.select(col("entity_id"),from_unixtime(unix_timestamp()),col("mid"))
      //.distinct()
      .select("entity_id")
      .distinct()
  }

  def getMerchantData(offlineOrganizedMerchant: DataFrame, eidUidMapper: DataFrame, offlineRetailer: DataFrame,
    merchants: DataFrame, entityInfo: DataFrame, entityDemographics: DataFrame,
    entityPreferences: DataFrame): DataFrame = {

    import offlineOrganizedMerchant.sparkSession.implicits._

    val exlusionList = entityExlusionList(entityDemographics, entityInfo)

    // Given by the Abu/Atul/Business/Rahul
    val entityPref = entityPreferences
      .where($"status" === 9376503)
      .where($"pref_type" === "34577774427".toLong)
      .where($"pref_value" === "34577774428".toLong)
      .select("entity_id").distinct()

    /// Make data
    val offlineMerchants = offlineOrganizedMerchant
      .join(eidUidMapper, Seq("entity_id"), "left_outer")
      .join(entityPref, Seq("entity_id"), "left_semi")
      .join(
        merchants.where($"merchant_id".isNotNull).select("merchant_id", "signup_date"),
        Seq("merchant_id"), "left_outer"
      )
      .select(
        $"customer_id",
        $"mid",
        $"isMerchant",
        $"merchant_first_name",
        $"merchant_last_name",
        $"merchant_email_ID",
        $"merchant_L1_type",
        $"merchant_L2_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"merchant_id",
        $"signup_date",
        $"created_on",
        $"expire_on"
      ).where($"merchant_L2_type".isNotNull)

    val onlineMerchants: DataFrame = entityInfo
      .join(entityDemographics.where($"entity_id".isNotNull), "entity_id")
      .join(merchants.where($"mid".isNotNull), Seq("mid"), "left_outer")
      .join(eidUidMapper.where($"entity_id".isNotNull), Seq("entity_id"), "left_outer")
      .join(exlusionList, Seq("entity_id"), "left_anti")
      .join(offlineOrganizedMerchant
        .select("entity_id")
        .where($"entity_id".isNotNull), Seq("entity_id"), "left_anti")
      .join(entityPref, Seq("entity_id"), "left_anti")
      .select(
        $"customer_id",
        $"mid",
        $"isMerchant",
        $"merchant_first_name",
        $"merchant_last_name",
        $"merchant_email_ID",
        $"merchant_L1_type",
        $"merchant_L2_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"merchant_id",
        $"signup_date",
        $"created_on",
        $"expire_on"
      )

    val p2pMerchants = offlineRetailer
      .join(offlineMerchants.select("customer_id").where($"customer_id".isNotNull).distinct, Seq("customer_id"), "left_anti")
      .join(onlineMerchants.select("customer_id").where($"customer_id".isNotNull).distinct, Seq("customer_id"), "left_anti")
      .select(
        $"customer_id",
        $"mid",
        $"isMerchant",
        $"merchant_first_name",
        $"merchant_last_name",
        $"merchant_email_ID",
        $"merchant_L1_type",
        $"merchant_L2_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"merchant_id",
        $"signup_date",
        $"signup_date".as("created_on"),
        current_timestamp().as("expire_on")
      )

    onlineMerchants
      .union(offlineMerchants)
      .union(p2pMerchants)
  }

  def getMerchantUniqueBy(merchants: DataFrame, id: String): DataFrame = {

    // Remove duplicated due to a multiple mappings across merchant_id,mid,customer_id
    val reqColms = Seq("mid", "merchant_L1_type", "merchant_L2_type", "merchant_category", "merchant_subcategory",
      "signup_date", "created_on", "expire_on")
    val aggColNames = reqColms.diff(Seq(id))
    val aggColms = aggColNames.map(colName => first(colName).as(colName))

    merchants
      //.where(col("merchant_L1_type") === "OFFLINE") //TODO: remove when other merchants need to be enabled.
      .where(col(id).isNotNull)
      .groupBy(id)
      .agg(aggColms.head, aggColms: _*)
      .select(id, aggColNames: _*)
  }

  def getNewSTRPaymentsTable(newSTRTable: DataFrame, merchantData: DataFrame, offlineRFSE: DataFrame): DataFrame = {

    import merchantData.sparkSession.implicits._

    val merchants_customerId = getMerchantUniqueBy(merchantData, "customer_id")
      .where($"merchant_L1_type" === "OFFLINE")

    val P2PTxn = newSTRTable
      .where($"txn_amount" > 5)
      .where($"txn_type".isin(5, 69))
      .where($"customer_id" =!= $"payee_id")
      .where($"payee_id".isNotNull)
      .join(broadcast(merchants_customerId), merchants_customerId("customer_id") === newSTRTable("payee_id"), "inner")
      .join(offlineRFSE, Seq("customer_id"), "left_anti")
      .where(($"created_at" >= $"signup_date") && $"created_at".between($"created_on", $"expire_on"))
      .select(newSTRTable("customer_id"), lit("P2P").as("payment_type"),
        $"str_id".as("order_item_id"),
        $"txn_amount".as("selling_price"),
        $"merchant_L2_type",
        $"merchant_L1_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"dt", $"created_at",
        $"mid",
        $"alipay_trans_id",
        $"byUPI",
        $"byNB",
        lit(null).alias("is_pg_drop_off"),
        lit(null).alias("payment_mode"),
        lit(null).alias("request_type"),
        $"successfulTxnFlag")

    val merchants_merchantId = getMerchantUniqueBy(merchantData, "merchant_id")
    val MIDPTxn = newSTRTable
      .where($"txn_type" === 1)
      .where($"payee_id".isNotNull)
      .join(broadcast(merchants_merchantId), merchants_merchantId("merchant_id") === newSTRTable("payee_id"), "inner")
      .where($"created_at" >= $"signup_date")
      .where(($"merchant_L1_type" === "ONLINE") || ($"txn_amount" > 5))
      .select(newSTRTable("customer_id"), lit("MIDP").as("payment_type"),
        $"str_id".as("order_item_id"),
        $"txn_amount".as("selling_price"),
        $"merchant_L2_type",
        $"merchant_L1_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"dt", $"created_at",
        $"mid",
        $"alipay_trans_id",
        $"byUPI",
        $"byNB",
        lit(null).alias("is_pg_drop_off"),
        lit(null).alias("payment_mode"),
        lit(null).alias("request_type"),
        $"successfulTxnFlag")

    MIDPTxn.union(P2PTxn)
  }

  def getAlipayOlapPaymentsTable(alipayOlap: DataFrame, merchantData: DataFrame): DataFrame = {

    import merchantData.sparkSession.implicits._

    val merchants = getMerchantUniqueBy(merchantData, "mid")
      .where($"merchant_L1_type" === "OFFLINE")

    val alipayTxn = alipayOlap
      .where($"txn_amount" > 5)
      .where($"txn_status" === "SUCCESS")
      .where($"mid".isNotNull)
      .join(broadcast(merchants), Seq("mid"), "inner")
      .where($"created_at" >= $"signup_date")
      .select(
        $"customer_id",
        lit("MIDP").as("payment_type"),
        $"alipay_trans_id".as("order_item_id"),
        $"txn_amount".as("selling_price"),
        $"merchant_L2_type",
        $"merchant_L1_type",
        $"merchant_category",
        $"merchant_subcategory",
        $"dt",
        $"created_at",
        $"mid",
        $"alipay_trans_id",
        when($"payment_type" === "UPI", 1).otherwise(0).alias("byUPI"),
        when($"payment_type" === "NET_BANKING", 1).otherwise(0).alias("byNB"),
        $"payment_mode",
        lit(1).alias("successfulTxnFlag"),
        when($"responsecode" isin (401, 810), 1).otherwise(null).alias("is_pg_drop_off"),
        $"request_type"
      )
    alipayTxn
  }

  def deviceSignalTable(spark: SparkSession, signalEventsPath: String, date: String): DataFrame = {
    import spark.implicits._

    val signalPath = s"$signalEventsPath/mobile-device-signals/dt=$date"

    val deviceSignalSchema = StructType(Seq(
      StructField("appVersion", StringType),
      StructField("clientId", StringType),
      StructField("customerId", StringType),
      StructField("dataTime", StringType),
      StructField("dateTime", StringType),
      StructField("deviceDateTime", StringType),
      StructField("uploadTime", StringType),
      StructField("deviceId", StringType),
      StructField("eventGuid", StringType),
      StructField("eventType", StringType),
      StructField("messageVersion", IntegerType),
      StructField("payload", StringType)
    ))

    spark
      .read
      .schema(deviceSignalSchema)
      .json(signalPath)
      .withColumn("dataTime", $"dataTime".cast(LongType))
      .withColumn("deviceDateTime", $"DeviceDateTime".cast(LongType))
      .withColumn("dateTime", $"dateTime".cast(LongType))
      .withColumn("uploadTime", $"uploadTime".cast(LongType))
  }

  def ticketnewUsers(spark: SparkSession, ticketnewUsersPath: String): DataFrame = {
    import com.paytm.map.features.utils.ConvenientFrame._

    val usersDaily = spark.read.option("header", "true")
      .csv(s"$ticketnewUsersPath/users.*.csv")

    val usersAnnual = spark.read.option("header", "true")
      .csv(s"$ticketnewUsersPath/online_users_*.csv")
      .drop("City")

    usersDaily
      .select(usersDaily.columns.map(x => col(x).as(x.toLowerCase)): _*)
      .safeUnion(usersAnnual.select(usersAnnual.columns.map(x => col(x).as(x.toLowerCase)): _*))
      .select("user_id", "user_email", "user_phone", "first_name", "last_name", "created_datetime")
  }

  def insiderUserTransactions(spark: SparkSession, ticketnewUsersPath: String): DataFrame = {
    spark.read.option("header", "true").csv(s"$ticketnewUsersPath/insider_*.csv")
      .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp"), "yyyy-mm-dd hh:MM:ss"))
      .select("transaction_id", "transaction_timestamp", "delivery_email", "delivery_name", "delivery_tel")
  }

  def lmsAccountV2(spark: SparkSession, lmsAccountsV2: DataFrame, lmsCasesV2: DataFrame): DataFrame = {
    import spark.implicits._

    val toLMSAccount = udf((
      account_number: String,
      total_amount: Double,
      balance_amount: Double,
      due_date: Timestamp,
      late_fee: Double,
      product_source: String,
      product_type: String,
      status: Int,
      dpd: Int
    ) =>
      LMSAccountV2(
        AccountNumber = account_number,
        TotalAmount   = total_amount,
        BalanceAmount = balance_amount,
        DueDate       = due_date,
        LateFee       = late_fee,
        ProductSource = product_source,
        ProductType   = product_type,
        Status        = status,
        DPD           = dpd
      ))

    lmsAccountsV2
      .join(broadcast(lmsCasesV2), Seq("account_number", "product_type"))
      .select(
        $"user_id", //can either be customer_id or merchant_id
        struct($"created_at", $"updated_at", $"account_number", $"due_date", $"total_amount", $"balance_amount", $"late_fee", $"product_source", $"product_type", $"status", $"dpd") as "d"
      )
      .withColumn("account_details", toLMSAccount(
        $"d.account_number", $"d.total_amount", $"d.balance_amount", $"d.due_date".cast(DateType), $"d.late_fee", $"d.product_source", $"d.product_type", $"d.status", $"d.dpd"
      ))
      .select($"user_id", $"account_details", $"d.*")
  }

  /********************************************************************************************************************/

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Functionalities for aggregates
  implicit class DFCommons(dF: DataFrame) {

    // Variable definitions //
    //soi aggregatation
    private val txnAggregates = Seq(
      count(col("order_item_id")).as("total_transaction_count"),
      sum(col("selling_price")).as("total_transaction_size"),
      min(col("created_at")).as("first_transaction_date"),
      max(col("created_at")).as("last_transaction_date")
    )

    private val lastTxnDateAggregates = Seq(
      max(col("created_at")).as("last_attempt_order_date")
    )
    // Discount Features
    private val discountAggregates = Seq(
      count(when(col("discount") > 0, col("order_item_id")).otherwise(null)).as("total_discount_transaction_count"),
      sum(when(col("discount") > 0, col("discount")).otherwise(null)).as("total_discount_transaction_size")
    )
    //promocode Features
    private val promocCodeAggregates = Seq(
      count(col("order_item_id")).as("total_cashback_transaction_count"),
      sum(col("amount")).as("total_cashback_transaction_size")
    )
    //promocode usage Features
    private val promoUsageAggregates = Seq(
      collect_set(col("promo_date")).as("promocodes_used")
    )
    //first last aggregates
    private val firstLastAggregate = Seq(
      first(col("first_transaction_size")).as("first_transaction_size"),
      first(col("last_transaction_size")).as("last_transaction_size")
    )
    //operator aggregates
    private val OperatorAggregates = Seq(
      first(col("first_transaction_operator")).as("first_transaction_operator"),
      first(col("last_transaction_operator")).as("last_transaction_operator")
    )
    //Bill due date aggregated
    private val BillAggregates = Seq(
      collect_set(col("operator_due_dates")).as("operator_due_dates")
    )
    //SMS aggregated
    private val SMSAggregates = Seq(
      collect_set(col("sender_name_and_time")).as("sender_name_and_time")
    )
    // first last column with window functions
    private def firstLastCol(w: WindowSpec) = Seq(
      first(col("selling_price"), ignoreNulls = true).over(w.orderBy(col("created_at"))).as("first_transaction_size"),
      first(col("selling_price"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_transaction_size")
    )
    //first last operator columns
    private def operatorCol(w: WindowSpec) = Seq(
      first(col("operator"), ignoreNulls = true).over(w.orderBy(col("created_at"))).as("first_transaction_operator"),
      first(col("operator"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_transaction_operator")
    )

    // ga aggregates
    private val gaAggregates = Seq(
      sum(col("product_views_app")).as("total_product_views_app"),
      sum(col("product_views_web")).as("total_product_views_web"),
      sum(col("product_clicks_app")).as("total_product_clicks_app"),
      sum(col("product_clicks_web")).as("total_product_clicks_web"),
      sum(col("pdp_sessions_app")).as("total_pdp_sessions_app"),
      sum(col("pdp_sessions_web")).as("total_pdp_sessions_web")
    )

    // ga lp (landing page) aggregates
    private val gaLPAggregates = Seq(
      sum(col("clp_sessions_app")).as("total_clp_sessions_app"),
      sum(col("clp_sessions_web")).as("total_clp_sessions_web"),
      sum(col("glp_sessions_app")).as("total_glp_sessions_app"),
      sum(col("glp_sessions_web")).as("total_glp_sessions_web")
    )

    private val superCashBackAggregates = Seq(
      count(when(col("status") === 6, 1).otherwise(null)).alias("current_unAck_count"),
      count(when(col("status") === 1, 1).otherwise(null)).alias("current_initialized_count"),
      count(when(col("status") === 2, 1).otherwise(null)).alias("current_inprogress_count"),
      count(when(col("status") === 3, 1).otherwise(null)).alias("current_completed_count"),
      count(when(col("status") === 5, 1).otherwise(null)).alias("current_expired_count"),
      count("campaign_id").alias("total_instance_count"),
      max(when(col("status") === 3, to_date(col("updated_at"))).otherwise(null)).alias("last_camp_completed_date"),
      max(when(col("status") === 5, to_date(col("expire_at"))).otherwise(null)).alias("last_camp_expired_date"),
      max(when(col("status") === 2, to_date(col("updated_at"))).otherwise(null)).alias("current_init_inprog_start_date"),
      max(when(col("status") === 2, to_date(col("expire_at"))).otherwise(null)).alias("current_init_inprog_expiry_date"),
      max(when(col("status") === 2, col("txn_count")) otherwise (null)).alias("current_inprogress_stage"), // txn_count column has stage data (https://jira.mypaytm.com/browse/MAP-1036)
      max(when(col("status") === 2 and col("txn_count") > 0, to_date(col("updated_at"))).otherwise(null)).alias("current_inprog_last_txn_date")
    )

    // function to get grouped-pivotted dataset
    def getGroupedDataSet(groupByCol: Seq[String], pivotCol: String, pivotValues: Seq[String]): RelationalGroupedDataset = {
      pivotCol match {
        case "" => dF.groupBy(groupByCol.map(col): _*)
        case pivotCol: String => {
          if (pivotValues.isEmpty)
            dF.groupBy(groupByCol.map(col): _*).pivot(pivotCol)
          else
            dF.groupBy(groupByCol.map(col): _*).pivot(pivotCol, pivotValues)
        }
        case _ => dF.groupBy(groupByCol.map(col): _*)
      }
    }

    // functions to add append columns.
    def addFirstLastCol(partitionCol: Seq[String], isOperator: Boolean = false): DataFrame = {

      val w = Window.partitionBy(partitionCol.head, partitionCol.tail: _*)
      val selectCol = partitionCol.map(col) ++ firstLastCol(w) ++ (if (isOperator) operatorCol(w) else Seq[Column]())
      dF.select(selectCol: _*).distinct
    }

    def addPreferredOperatorFeat(levelCol: String): DataFrame = {
      // Preferred Operator Count
      def getOperatorCnt = udf[OperatorCnt, String, Long]((opt: String, cnt: Long) => OperatorCnt(opt, cnt))

      import dF.sparkSession.implicits._
      dF.where($"operator".isNotNull && ($"operator" =!= ""))
        .groupBy("customer_id", "dt", "operator", levelCol)
        .count()
        .groupBy("customer_id", "dt")
        .pivot(levelCol)
        .agg(
          collect_list(getOperatorCnt($"operator", $"count")).as("preferred_operator")
        )
    }

    def addPreferredBrandFeat(levelCol: String): DataFrame = {
      // Preferred Brand Count
      def getBrandCnt = udf[OperatorCnt, String, Long]((opt: String, cnt: Long) => OperatorCnt(opt, cnt))

      import dF.sparkSession.implicits._
      dF.where($"brand_id".isNotNull)
        .withColumn("brand_id", $"brand_id".cast(StringType))
        .groupBy("customer_id", "dt", "brand_id", levelCol)
        .agg(
          count($"customer_id").as("count"),
          sum($"selling_price" * 100).cast(LongType).as("txn_size")
        )
        .groupBy("customer_id", "dt")
        .pivot(levelCol)
        .agg(
          collect_list(getBrandCnt($"brand_id", $"count")).as("preferred_brand_count"),
          collect_list(getBrandCnt($"brand_id", $"txn_size")).as("preferred_brand_size")
        )
    }

    def addPreferredColumnsFeat(levelCol: String, columnNamesAliases: Seq[(String, String)]): DataFrame = {

      val aggColumnAndAlias = if (columnNamesAliases.size == 1) columnNamesAliases :+ (columnNamesAliases.head._1, "dummy")
      else columnNamesAliases

      val aggColumns = aggColumnAndAlias.map { colM =>
        dF.schema(colM._1).dataType match {
          case _: ArrayType => toOperatorCnt(flattenSeqOfSeq(collect_list(col(colM._1)))).as(colM._2)
          case _            => toOperatorCnt(collect_list(col(colM._1))).as(colM._2)
        }
      }

      dF.groupBy("customer_id", "dt")
        .pivot(levelCol)
        .agg(aggColumns.head, aggColumns.tail: _*)
        .drop("dummy")
    }

    def addSalesAggregates(
      groupByCol: Seq[String],
      pivotCol: String = "",
      isDiscount: Boolean = true,
      pivotValues: Seq[String] = Seq(),
      unFiltered: Boolean = false
    ): DataFrame = {
      val aggCol = if (unFiltered) {
        lastTxnDateAggregates
      } else if (isDiscount) {
        txnAggregates ++ discountAggregates
      } else txnAggregates

      groupPivotAgg(groupByCol, pivotCol, aggCol, pivotValues = pivotValues)
    }

    def addBillsAggregates(groupByCol: Seq[String], pivotCol: String = ""): DataFrame = {
      groupPivotAgg(groupByCol, pivotCol, BillAggregates)
    }

    def addSMSAggregates(groupByCol: Seq[String]): DataFrame = groupPivotAgg(groupByCol, "", SMSAggregates)

    def addPromocodeAggregates(groupByCol: Seq[String], pivotCol: String = "", isL1: Boolean = false): DataFrame = {
      val aggCols = if (isL1) promocCodeAggregates ++ promoUsageAggregates else promocCodeAggregates
      groupPivotAgg(groupByCol, pivotCol, aggCols)
    }

    def addFirstLastAggregates(groupByCol: Seq[String], pivotCol: String = "", isOperator: Boolean = false, pivotValues: Seq[String] = Seq()): DataFrame = {
      val aggCol = if (isOperator) firstLastAggregate ++ OperatorAggregates else firstLastAggregate
      groupPivotAgg(groupByCol, pivotCol, aggCol, pivotValues = pivotValues)
    }

    def addGAAggregates(groupByCol: Seq[String], pivotCol: String = ""): DataFrame = {
      groupPivotAgg(groupByCol, pivotCol, gaAggregates)
    }

    def addSuperCashBackAggregates(groupByCol: Seq[String], pivotCol: String = ""): DataFrame = {
      groupPivotAgg(groupByCol, pivotCol, superCashBackAggregates)
    }

    def addGALPAggregates(groupByCol: Seq[String], pivotCol: String = ""): DataFrame = {
      groupPivotAgg(groupByCol, pivotCol, gaLPAggregates)
    }
    def groupPivotAgg(groupByCol: Seq[String], pivotCol: String, aggCol: Seq[Column], pivotValues: Seq[String] = Seq()): DataFrame = {
      dF.getGroupedDataSet(groupByCol, pivotCol, pivotValues)
        .agg(aggCol.head, aggCol.tail: _*)
    }

    def getWalletTxns(onUsMID: Array[String]): DataFrame = {
      import dF.sparkSession.implicits._

      dF.
        withColumn("isOnPaytm", when($"payee_id" isin (onUsMID: _*), 1).otherwise(0)).
        filter($"isP2P" === 1 ||
          $"isP2B" === 1 ||
          ($"isUserToMerchant" === 1 && $"isOnPaytm" === 0)) // Off Paytm txns
        .select(
          $"customer_id",
          $"dt",
          $"merchant_order_id" as "order_id",
          $"str_id" as "order_item_id",
          $"txn_amount" as "selling_price",
          $"created_at",
          lit(0.0) as "discount" // No discounts in case of wallet
        )
    }

    // Utility functions
    def alignSchema(schema: StructType, requiresExistence: Boolean = false): Dataset[Row] = {
      val existingColumns = dF.columns
      val allColumns = schema.map(field => {
        requiresExistence match {
          case true  => (if (existingColumns.contains(field.name)) col(field.name) else throw new Exception(s"Field name ${field.name} not present in data when aligning schema")).cast(field.dataType).as(field.name)
          case false => (if (existingColumns.contains(field.name)) col(field.name) else lit(null)).cast(field.dataType).as(field.name)
        }
      })
      dF.select(allColumns: _*)
    }

    def renameColumns(prefix: String, excludedColumns: Seq[String]): DataFrame = {

      val columnsRenamed = dF.columns
        .map(colM => if (excludedColumns.contains(colM)) dF.col(colM) else dF.col(colM).as(prefix + colM))
        .toSeq

      dF.select(columnsRenamed: _*)
    }

    def moveHDFSData(dtSeq: Seq[String], destBasePath: String): Unit = {
      dtSeq.par.foreach(dt => {
        dF.where(col("dt") === dt).write.mode(SaveMode.Overwrite).parquet(s"$destBasePath/dt=$dt")
      })
    }

  }

}
