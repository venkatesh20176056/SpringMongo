package com.paytm.map.features.datasets.hero

import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.joda.time.DateTime

object ReadHeroTables {

  private val settings = new Settings
  private val config = settings.rootConfig.getConfig("dfs_datalake")
  private val apiBasePath = config.getString("snapshotQueryEndPointV3")

  val defaultMinDate: DateTime = new DateTime("2017-03-02")

  val trimQuotations = udf((weType: String) => {
    weType.stripPrefix("\"").stripSuffix("\"")
  })

  def loadDataFromApi(
    spark: SparkSession,
    tableName: String
  ): DataFrame = {

    import org.apache.http.client.methods.HttpGet
    import org.apache.http.impl.client.HttpClientBuilder
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    import scala.io.Source
    implicit val formats = DefaultFormats

    val httpClient = HttpClientBuilder.create().build()
    val httpResponse = httpClient.execute(new HttpGet(apiBasePath + tableName + "/manifest/"))
    val entity = httpResponse.getEntity

    val paths: Array[String] = {
      val inputStream = entity.getContent
      val content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close()
      httpClient.close()

      parse(content).extract[Map[String, Any]].get("partitions").get.asInstanceOf[Map[String, String]].values.toArray
    }.map(x => x.replace("s3:", "s3a:"))

    spark.read.parquet(paths: _*)
  }

  def readHeroTables(
    tableName: String,
    toDate: DateTime,
    fromDate: DateTime = defaultMinDate
  )(implicit spark: SparkSession): DataFrame = {

    val toDateStr = toDate.plusDays(1).toString(ArgsUtils.formatter)
    val fromDateStr = fromDate.toString(ArgsUtils.formatter)

    loadDataFromApi(spark, tableName = tableName)
      .withColumn("created_at", from_utc_timestamp(col("created_at"), "EST"))
      .filter(col("created_at").between(fromDateStr, toDateStr))
  }

  def filterBaseTableTimeSeries(
    baseTable: DataFrame,
    toDate: DateTime
  ): Seq[(DataFrame, Int)] = {

    val durations = timeSeriesDurations
    val fromDates = durations.map(toDate.minusDays)

    val timeSeriesDFs = for (fromDate <- fromDates) yield {
      val fromDateStr = fromDate.toString(ArgsUtils.formatter)
      baseTable.filter(col("dt") >= fromDateStr)
    }
    timeSeriesDFs.zip(durations)
  }

  def getHeroCustomersTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val blockedCondition = not(col("admin_block_state").isin("default")) ||
      not(col("fraud_block_state").isin("default")) ||
      not(col("terrorist_block_state").isin("default", "manual_whitelist"))

    val deletedCondition = when($"state" === "DEACTIVATED", 1).otherwise(0)

    val prodUserUser = readHeroTables("prod_user.user", toDate, fromDate)
      .select(
        $"id" as "customer_id",
        lower(get_json_object($"temporary_sign_in_id", "$.signInMethods[0].signInId")) as "sign_in_email",
        $"email_verified_at",
        $"state",
        $"created_at" as "customer_created_at"
      )

    val heroProdCustomer = readHeroTables("hero_prod.customers", toDate, fromDate)
      .select(
        $"id" as "customer_id",
        lower($"email") as "hero_prod_email",
        $"country_code",
        year($"dob") as "year_of_birth",
        $"score" as "risk_score",
        $"notification_flags",
        $"kyc_verified",
        $"kyc_verified_at",
        $"first_name",
        $"last_name",
        $"referrer_code",
        when(blockedCondition, 1).otherwise(0) as "blocked"
      )

    prodUserUser
      .join(heroProdCustomer, Seq("customer_id"), "left_outer")
      .withColumn("deleted", deletedCondition)
      .withColumn("email_id", coalesce($"sign_in_email", $"hero_prod_email"))
      .withColumn("email", $"email_id")
  }

  def getUserPhonesTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    def cleanPhoneNumber(phoneCol: Column): Column = regexp_replace(phoneCol, "[^0-9]", "")

    val phonesAll = readHeroTables("prod_user.phone", toDate, fromDate)
      .withColumnRenamed("user_id", "customer_id")
      .cache

    val phoneCount = phonesAll
      .groupBy("customer_id")
      .agg(
        min("verified_at") as "min_phone_verified_at",
        count("number") as "count_phone_numbers"
      )

    val phoneNumber = phonesAll
      .withColumn("phone_number", cleanPhoneNumber(get_json_object($"validation_response", "$.nationalFormat")))
      .getFirstLast(Seq("customer_id"), "id", Seq("phone_number as number"), isFirst = false)

    phoneCount
      .join(phoneNumber, Seq("customer_id"), "left_outer")
  }

  def getHeroAddressesTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.addresses", toDate, fromDate)
      .dropDuplicates("customer_id")
      .select(
        $"city" as "address_city",
        $"state" as "province_state",
        $"country",
        $"customer_id"
      )
  }

  def getHeroCustLocTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.customer_location_tracks", toDate, fromDate)
      .dropDuplicates("customer_id")
      .select(
        $"customer_id",
        $"latitude",
        $"longitude",
        $"city" as "ip_location_city"
      )
      .withColumn("latitude", round($"latitude".cast("double"), 2))
      .withColumn("longitude", round($"longitude".cast("double"), 2))
  }

  def getHeroOrderMetaTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import com.paytm.map.features.utils.ConvenientFrame._
    import spark.implicits._

    readHeroTables("hero_prod.order_meta", toDate, fromDate)
      .select(
        $"customer_id",
        $"order_id",
        $"os_type"
      )
      .getFirstLast(Seq("customer_id"), "order_id", Seq("os_type as os_type"), isFirst = false)
  }

  def getHeroPaymentMethodsTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.payment_methods", toDate, fromDate)
      .filter($"deleted_at".isNull)
      .select(
        $"id" as "payment_method_id",
        $"customer_id",
        $"type",
        $"created_at" as "pm_created_at",
        $"deleted_at",
        lower($"brand") as "brand",
        $"dual_deposit",
        $"institution_number",
        $"last4",
        $"exp_month",
        $"exp_year"
      )
  }

  def getHeroBankProvidersTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.bank_providers", toDate, fromDate)
      .select(
        $"provider_id",
        $"provider_name",
        $"institution_number"
      )
  }

  def getProdWalletWalletTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("prod_wallet.wallet", toDate, fromDate)
      .select(
        $"id" as "wallet_id",
        $"user_id" as "customer_id",
        $"type" as "wallet_type"
      )
  }

  def getProdWalletAccountTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("prod_wallet.account", toDate, fromDate)
      .select(
        $"id" as "account_id",
        $"wallet_id",
        $"cents" / 100 as "current_wallet_balance",
        $"currency", $"type" as "account_type",
        get_json_object($"metadata", "$.campaignToken") as "campaign_token"
      )
  }

  def getProdWalletTxnTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("prod_wallet.transaction", toDate, fromDate)
      .select(
        $"id" as "txn_id",
        $"src_user_id",
        $"dst_user_id" as "customer_id",
        $"src_account_uuid",
        $"dst_amount" / 100 as "dst_amount_dollars",
        $"src_amount" / 100 as "src_amount_dollars",
        $"src_account_type",
        $"dst_account_type"
      )
  }

  def getHeroOrdersTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.orders", toDate, fromDate)
      .select(
        $"id" as "order_id",
        $"customer_id",
        $"payment_method_id",
        $"country",
        $"created_at" as "order_created_at",
        $"subtotal_cents" / 100 as "order_subtotal",
        $"total_cents" / 100 as "order_total",
        $"state" as "order_state",
        $"promo_code",
        $"promo_amount_cents" / 100 as "promo_amount"
      )
      .withColumn("time_of_day", hour($"order_created_at") + minute($"order_created_at") / 60)
  }

  def getHeroOrdersItemsTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.order_items", toDate, fromDate)
      .select(
        $"id" as "order_item_id",
        $"shipment_id",
        $"item_id",
        $"item_type",
        $"quantity",
        $"order_id"
      )
  }

  def getHeroProductVariantsTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.product_variants", toDate, fromDate)
      .select(
        $"id" as "product_variant_id",
        $"name" as "product_variant_name",
        $"price_cents" / 100 as "product_variant_price"
      )
      .withColumn("product_variant_name", regexp_replace($"product_variant_name", "®", ""))
      .withColumn("gift_card_name_amount", concat_ws(" ", $"product_variant_name", format_number($"product_variant_price", 0).cast("string")))
  }

  def getHeroBillersTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.billers", toDate, fromDate)
      .select(
        $"id" as "biller_id",
        $"code" as "biller_code",
        $"state" as "biller_state",
        $"name" as "biller_name",
        $"biller_category_id"
      )
  }

  def getHeroBillerCatTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.biller_categories", toDate, fromDate)
      .select(
        $"id" as "biller_category_id",
        $"name" as "biller_category_name",
        $"state" as "biller_category_state"
      )
  }

  def getHeroPaymentsTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.payments", toDate, fromDate)
      .select(
        $"id" as "payments_id",
        $"order_id",
        $"type" as "payments_type",
        $"amount_cents" / 100 as "payments_amount",
        $"state" as "payments_state",
        $"wallet_cents" / 100 as "wallet_dollars",
        $"pad_cents" / 100 as "pad_dollars",
        $"credit_card_cents" / 100 as "cc_dollars"
      )
      .filter($"payments_type" =!= "CombinedPayment")
  }

  def getHeroPayeesTable(toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.payees", toDate, fromDate)
      .select(
        $"id" as "payee_id",
        $"customer_id",
        $"type" as "payee_type",
        $"country_code" as "payee_country_code",
        $"created_at" as "payee_created_at",
        $"operator",
        $"deleted_at",
        $"product_id" as "biller_code",
        $"frequency",
        $"link_state"
      )
  }

  def getHeroLoyaltyPoints(settings: Settings, toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.loyalty_wallets", toDate, fromDate)
      .select(
        $"customer_id",
        $"balance".cast("long") as "loyalty_points"
      )
  }

  def getCustomerScoreHistories(settings: Settings, toDate: DateTime, fromDate: DateTime = defaultMinDate)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readHeroTables("hero_prod.customer_score_histories", toDate, fromDate)
      .select(
        $"customer_id",
        $"old_score",
        $"new_score",
        $"created_at"
      )
  }

}
