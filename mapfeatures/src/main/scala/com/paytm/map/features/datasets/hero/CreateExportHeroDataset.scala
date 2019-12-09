package com.paytm.map.features.datasets.hero

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.campaigns.CampaignConstants._
import com.paytm.map.features.datasets.hero.HeroConstants._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Record(timestamp: String, customer_id: Long, provider_id: String, status: String)
case class ScoreRecord(customer_id: Long, old_score: Int, new_score: Int, created_at: String)

object CreateExportHeroDataset extends CreateExportHeroDatasetJob with SparkJob with SparkJobBootstrap

trait CreateExportHeroDatasetJob {
  this: SparkJob =>

  val JobName = "CreateExportHeroDataset"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import settings.{featuresDfs, heroFeaturesES}
    import spark.implicits._
    implicit val sparkSession: SparkSession = spark
    spark.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val targetDateTimeStr = targetDate.toString("YYYY-MM-dd HH:mm:ss")
    val ordersMinDate: DateTime = new DateTime("2017-03-16")
    val daySeconds = 60 * 60 * 24
    val timeSeriesDays = getTimeSeriesDays
    val groupPrefix = args(1)
    val logsFormatPattern = "yyyy.MM.dd"
    val logsFormatter = DateTimeFormat.forPattern(logsFormatPattern)
    val numOfDaysForBankLinks = 7
    val scoreThreshold = 675

    // Constants
    val byCustomer = getHeroKey
    val byOrder = getGroupByOrder
    val defaultJoin = "left_outer"
    val deletedFilter = getDeletedFilter
    val walletFilter = getWalletFilters
    val paymentsFilters = getPaymentsFilters
    val orderFilters = getOrderFilters
    val createdFilter = getCreatedFilters
    val completedFilter = getCompletedFilters
    val failedFilter = getFailedFilters
    val createdAgg = getHeroAggregations("created")
    val failedAgg = getHeroAggregations("failed")
    val payeesCond = getHeroConditions("payees")
    val paymentsCond = getHeroConditions("payments")
    val paymentsAgg = getHeroAggregations("payments", paymentsCond)
    val resurrectedCond = getResurrectedConditions
    val createdAtCond = getHeroConditions("firstLast")
    val operatorCond = getOperatorConditions
    val segmentNullZeroCols = getSegmentNullZeroCols
    val heroDropCols = getDropCols
    val paymentsDropCols = getPaymentsCols
    val bankLinkAggregations = getBankLinkAggregations
    val expiringCCAggregations = getExpiringCCAggregations

    // Read all data frames
    val customersTemp = ReadHeroTables.getHeroCustomersTable(targetDate)
    customersTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/customers")
    val phonesTemp = ReadHeroTables.getUserPhonesTable(targetDate)
    phonesTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/phones")
    val addressesTemp = ReadHeroTables.getHeroAddressesTable(targetDate)
    addressesTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/addresses")
    val customerLocTrackTemp = ReadHeroTables.getHeroCustLocTable(targetDate)
    customerLocTrackTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/customerLocTrack")
    val orderMetaTemp = ReadHeroTables.getHeroOrderMetaTable(targetDate)
    orderMetaTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orderMeta")
    val paymentMethodsTemp = ReadHeroTables.getHeroPaymentMethodsTable(targetDate)
    paymentMethodsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/paymentMethods")
    val bankProvidersTemp = ReadHeroTables.getHeroBankProvidersTable(targetDate)
    bankProvidersTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/bankProviders")
    val walletWalletTemp = ReadHeroTables.getProdWalletWalletTable(targetDate)
    walletWalletTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletWallet")
    val walletAccountTemp = ReadHeroTables.getProdWalletAccountTable(targetDate)
    walletAccountTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletAccount")
    val walletTxnTemp = ReadHeroTables.getProdWalletTxnTable(targetDate)
    walletTxnTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletTxn")
    val payeesTemp = ReadHeroTables.getHeroPayeesTable(targetDate)
    payeesTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/payees")
    val paymentsTemp = ReadHeroTables.getHeroPaymentsTable(targetDate)
    paymentsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/payments")
    val orderItemsTemp = ReadHeroTables.getHeroOrdersItemsTable(targetDate)
    orderItemsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orderItems")
    val ordersTemp = ReadHeroTables.getHeroOrdersTable(targetDate, ordersMinDate)
    ordersTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orders")
    val billersTemp = ReadHeroTables.getHeroBillersTable(targetDate)
    billersTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/billers")
    val billerCatsTemp = ReadHeroTables.getHeroBillerCatTable(targetDate)
    billerCatsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/billerCats")
    val productVariantsTemp = ReadHeroTables.getHeroProductVariantsTable(targetDate)
    productVariantsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/productVariants")
    val loyaltyPointsTemp = ReadHeroTables.getHeroLoyaltyPoints(settings, targetDate)
    loyaltyPointsTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/loyaltyPoints")
    val scoreHistoriesTemp = ReadHeroTables.getCustomerScoreHistories(settings, targetDate)
    scoreHistoriesTemp.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/scoreHistories")

    val customers = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/customers")
    val phones = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/phones")
    val addresses = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/addresses")
    val customerLocTrack = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/customerLocTrack")
    val orderMeta = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orderMeta")
    val paymentMethods = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/paymentMethods")
    val bankProviders = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/bankProviders")
    val walletWallet = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletWallet")
    val walletAccount = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletAccount")
    val walletTxn = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/walletTxn")
    val payees = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/payees")
    val payments = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/payments")
    val orderItems = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orderItems")
    val orders = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/orders")
    val billers = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/billers")
    val billerCats = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/billerCats")
    val productVariants = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/productVariants")
    val loyaltyPoints = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/loyaltyPoints")
    val scoreHistories = spark.read.parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp/scoreHistories")

    /**
     * Takes data frame and applies case when and aggregation conditions above to output summary dataframe
     */
    def caseWhenAggregateSummary(
      joinedDataFrame: DataFrame,
      summaryTableName: String,
      groupByCustomer: Seq[String],
      summaryTableFilters: Seq[String]
    ): DataFrame = {

      val conditions = getHeroConditions(summaryTableName)
      val aggregations = getHeroAggregations(summaryTableName, conditions)

      joinedDataFrame
        .caseWhen(conditions)
        .filterGroupByAggregate(summaryTableFilters, groupByCustomer, aggregations)
    }

    /**
     * Function to get product purchase summary from orders data frame for each gift card or biller field name
     */
    def getProductDataFrame(
      productName: String,
      filters: Seq[String],
      ordersDf: DataFrame,
      groupBy: Seq[String],
      targetDate: String,
      days: Seq[Int]
    ): DataFrame = {

      val productFilters = getHeroProductFilters(productName, filters)
      val fieldName = getHeroProductFieldName(productName)
      val preAgg = getHeroPreAgg(productName)
      val firstLast = getHeroProductFirstLast(productName)

      val purchasePrefDfs = days.map(day => getPurchasePrefSummary(
        ordersDf.filter($"order_created_at" > date_add(lit(targetDateStr), -day)),
        productFilters,
        fieldName,
        preAgg,
        groupBy,
        day
      ))

      purchasePrefDfs.joinAllSmall(groupBy, "full_outer")
    }

    /**
     * Function to get the name of the product most recently purchased by the customer in each category
     */
    def getLatestProductName(firstLastOrderDf: DataFrame, orderFilters: Seq[String], productName: String): DataFrame = {

      val productDate = getHeroProductDate(productName)
      val filters = getHeroProductFilters(productName, orderFilters)
      val fieldAs = getHeroRecentFieldAs(productName)

      firstLastOrderDf
        .filter(filters.mkString(" and "))
        .getFirstLast(byCustomer, productDate, fieldAs, isFirst = false)
    }

    /**
     * Get a summary of all created orders for billers (or gift cards) from the past x days, where an order
     * was not attempted in those same days, take top 2 and make into string with "and"
     */
    def getCreatedSummary(
      spark: SparkSession,
      ordersDf: DataFrame,
      createdFilters: Seq[String],
      completedFilters: Seq[String],
      fieldName: String,
      custField: Seq[String],
      day: Int
    ): DataFrame = {

      val createdAgg = Seq(("collect_as_set", s"${fieldName} as created_orders_${fieldName}_${day}_days"))
      val createdWindow = Window.partitionBy("customer_id").orderBy(desc("created_count"))
      val purchaseCatGroupBy = custField :+ fieldName

      val created = ordersDf
        .filterGroupByAggregate(createdFilters, purchaseCatGroupBy, preAggCreated)

      val completed = ordersDf
        .filterGroupByAggregate(completedFilters, purchaseCatGroupBy, preAggCompleted)

      val createdNotCompleted = created
        .join(completed, (custField :+ fieldName), "left_outer")
        .filter($"completed_count".isNull)

      val topTwoCreated = createdNotCompleted
        .withColumn("row_num", row_number().over(createdWindow))
        .filter($"row_num" <= 2)

      val createdSet = topTwoCreated
        .filterGroupByAggregate(Seq(), custField, createdAgg)

      createdSet
        .withColumn(createdSet.columns(1), seqToAndString(col(createdSet.columns(1))))
        .withColumn("exists_" + createdSet.columns(1), when(col(createdSet.columns(1)).isNotNull, 1))
    }

    /**
     * Apply the getCreatedSummary function for each product and each number of look-back days
     */
    def getCreatedDataFrame(
      productName: String,
      createdFilters: Seq[String],
      completedFilters: Seq[String],
      ordersDf: DataFrame,
      groupBy: Seq[String],
      targetDate: String,
      days: Seq[Int]
    ): DataFrame = {

      val fieldName = getHeroProductFieldName(productName)
      val productCreatedFilters = getHeroProductFilters(productName, createdFilters)
      val productCompletedFilters = getHeroProductFilters(productName, completedFilters)

      val createdDfs = days.map(day => getCreatedSummary(
        spark,
        ordersDf.filter($"order_created_at" > date_add(lit(targetDate), -day)),
        productCreatedFilters,
        productCompletedFilters,
        fieldName,
        groupBy,
        day
      ))

      createdDfs.joinAllSmall(groupBy, "full_outer")
    }

    /**
     * Get customer profile information:
     * Join customers, phones, addresses, location, meta (OS info)
     * Create 1-0 flag columns indicating if information like email, address has been entered or veriied
     * Calculate days since events like customer creation, verification
     * Calculate approximate age using birth year
     */
    val customersJoined = Seq(customers, phones, addresses, customerLocTrack, orderMeta)
      .joinAllSmall(byCustomer, defaultJoin)

    val customerSummary = customersJoined
      .infoVerified("email_verified", $"email_verified_at", targetDateStr)
      .infoVerified("phone_verified", $"min_phone_verified_at", targetDateStr)
      .withColumn("address_entered", infoEntered($"address_city"))
      .withColumn("phone_entered", infoEntered($"count_phone_numbers"))
      .withColumn("days_since_signup", datediff(lit(targetDateStr), $"customer_created_at"))
      .withColumn("days_to_email_verification", datediff($"email_verified_at", $"customer_created_at"))
      .withColumn("days_to_phone_verification", datediff($"min_phone_verified_at", $"customer_created_at"))
      .withColumn("days_to_kyc_verification", datediff($"kyc_verified_at", $"customer_created_at"))
      .withColumn("age", year(lit(targetDateStr)) - $"year_of_birth")
      .withColumn("email_notifications_subscribed", emailSubscribed($"notification_flags"))

    /**
     * Get payment information:
     * Get information on credit cards and bank account types added by the customer, though not
     * necessarily used to make a transaction
     */
    val cleanedBankProviders = bankProviders
      .groupBy("institution_number")
      .agg(collect_set("provider_name") as "provider_name")
      .withColumn("provider_name", concat_ws(" or ", $"provider_name"))

    val paymentInfo = paymentMethods
      .join(cleanedBankProviders, Seq("institution_number"), defaultJoin)

    val paymentMethodSummary = caseWhenAggregateSummary(paymentInfo, "paymentMethod", byCustomer, deletedFilter)

    /**
     * Get wallet event information:
     */
    val walletBalanceSummary = walletAccount
      .filter($"account_type" === "EMONEY")
      .join(walletWallet.filter($"wallet_type" === "CUSTOMER"), Seq("wallet_id"))
      .select("customer_id", "current_wallet_balance")

    val walletTxnAccount = walletTxn
      .join(walletAccount, $"src_account_uuid" === $"account_id", "left_outer")
      .filter($"dst_account_type" === "EMONEY")

    val walletSummary = caseWhenAggregateSummary(walletTxnAccount, "wallet", byCustomer, walletFilter)
      .withColumn("preferred_funding_source", walletPrefType($"count_emt_txns", $"count_can_post_txns", $"count_pad_topup_txns"))

    /**
     * Get payees information:
     * Get information on all payees (billers) a customer has added
     */
    val payeesSummary = caseWhenAggregateSummary(payees, "payees", byCustomer, deletedFilter)

    /**
     * Get payments information:
     * Get payment methods used for each order (credit card, wallet, bank account, combined), to be subsequently
     * grouped by customer in order summary. Multiple payment methods can be used for one order.
     * Before paylite there are multiple rows for each order so needs to be grouped by order.
     * After paylite there is just one and payment type is by column (not including the "CombinedPayment" row)
     */

    val prePaylite = payments
      .filter($"payments_type" =!= "PaylitePayment")
      .dropAll(paymentsDropCols.head, paymentsDropCols.tail: _*)
      .caseWhen(paymentsCond)

    val paylite = payments
      .filter($"payments_type" === "PaylitePayment")

    val paymentsSummary = prePaylite
      .union(paylite)
      .filterGroupByAggregate(paymentsFilters, byOrder, paymentsAgg)
      .withColumn("combined_dollars", $"order_wallet_dollars" + $"order_pad_dollars" + $"order_cc_dollars")

    /**
     * Get order information:
     * Get payees table to get biller for each transaction
     * Join order items table (where "item_type" === "Payees::Base") with payees, billers, biller categories to get information on biller orders
     * Join order items table (where "item_type" === "Products::ProductVariant") with product variants to get information on gift cards
     * Union the two tables to get all order items for both product types
     * Join the main orders table with the order items to get order, customer information plus product details
     * Aggregate to get summary of order transactions like total spent, number of transactions etc
     */
    val payeesAll = payees
      .caseWhen(payeesCond)

    val billerOrders = orderItems
      .filter($"item_type" === "Payees::Base")
      .join(payeesAll, orderItems("item_id") === payees("payee_id"), defaultJoin)
      .join(billers, Seq("biller_code"), defaultJoin)
      .join(billerCats, Seq("biller_category_id"), defaultJoin)
      .withColumn("biller_name", when(operatorCond, $"operator").otherwise($"biller_name"))
      .withColumn("biller_category_name", when(operatorCond, lit("Indian_Operator")).otherwise($"biller_category_name"))
      .withColumn("canadian_biller", when($"payee_type" === "CanadianPayee", 1).otherwise(null))
      .withColumn("indian_biller", when($"payee_type" === "IndianMobileRechargePayee", 1).otherwise(null))

    val giftCardOrders = orderItems
      .filter($"item_type" === "Products::ProductVariant")
      .join(productVariants, orderItems("item_id") === productVariants("product_variant_id"), defaultJoin)
      .withColumn("gift_card", lit(1))

    val oiCols = orderItems.schema.fieldNames
    val orderItemsAll = billerOrders
      .join(giftCardOrders, oiCols, "full_outer")
      .drop("customer_id")

    val ordersJoined = orders
      .join(orderItemsAll, Seq("order_id"), defaultJoin)
      .join(paymentsSummary, Seq("order_id"), defaultJoin)

    val ordersSummary = caseWhenAggregateSummary(ordersJoined, "orders", byCustomer, orderFilters)

    /**
     * Get first last information:
     * Using orders table with details added, get a customer's first and most recent transactions
     * then get the name of gift card or biller for their most recent transaction
     */
    val firstLastOrder = caseWhenAggregateSummary(ordersJoined, "firstLast", byCustomer, orderFilters)

    val firstLastOrderAll = ordersJoined
      .caseWhen(createdAtCond)

    val latestBillOrder = getLatestProductName(firstLastOrderAll, orderFilters, "biller")

    val latestGiftCardOrder = getLatestProductName(firstLastOrderAll, orderFilters, "giftCard")

    val firstLastOrderSummary = Seq(firstLastOrder, latestBillOrder, latestGiftCardOrder).joinAllSmall(byCustomer, defaultJoin)

    /**
     * Get product information:
     * Get gift card and biller specific details using the orders info table, such as preferred biller / gift card,
     * all gift cards / billers purchased during different time periods
     * Join with transaction count for each biller category
     */
    val giftCardSummary = getProductDataFrame("giftCard", orderFilters, ordersJoined, byCustomer, targetDateStr, timeSeriesDays)
    val billerSummary = getProductDataFrame("biller", orderFilters, ordersJoined, byCustomer, targetDateStr, timeSeriesDays)
    val billerCatSummary = getProductDataFrame("billerCat", orderFilters, ordersJoined, byCustomer, targetDateStr, timeSeriesDays)

    val billerFilters = getHeroProductFilters("biller", orderFilters)
    val allCategorySummary = caseWhenAggregateSummary(ordersJoined, "billerCat", byCustomer, billerFilters)

    /**
     * Get created and failed order details:
     * Use order info table to filter on created and failed orders to get a count by customer
     */
    val createdSummary = ordersJoined
      .filterGroupByAggregate(createdFilter, byCustomer, createdAgg)

    val failedSummary = ordersJoined
      .filterGroupByAggregate(failedFilter, byCustomer, failedAgg)

    val billerCreatedSummary = getCreatedDataFrame("biller", createdFilter, completedFilter, ordersJoined, Seq("customer_id"), targetDateStr, createdDays)
    val gcCreatedSummary = getCreatedDataFrame("giftCard", createdFilter, completedFilter, ordersJoined, Seq("customer_id"), targetDateStr, createdDays)

    /**
     * Resurrected users:
     * For customer segment get resurrected customer flag - were inactive for 30 days and then made an order
     * again within the past month (after this they become repeat users again)
     * Use a window function to get time between orders, customers are resurrected where max time between >= 35 days,
     * and days since this resurrection < 35
     */
    // get max time between orders
    val windowCustCreated = HeroConstants.getOrdersWindow
    val ordersWindowAgg = HeroConstants.getOrdersWindowAgg

    val ordersWindow = orders
      .filter(orderFilters.mkString)
      .withColumn("previous_created_at", lag($"order_created_at", 1).over(windowCustCreated))
      .withColumn("time_between_orders", datediff($"order_created_at", $"previous_created_at"))
      .withColumn("days_since_resurrection", when($"time_between_orders" >= 35, datediff(lit(targetDateStr), $"order_created_at")))

    val resurrectedSummary = ordersWindow
      .filterGroupByAggregate(orderFilters, byCustomer, ordersWindowAgg)

    val campaignsPath = s"${featuresDfs.campaignsTable}/country=canada/"
    val campaignDays = canadaCampaignDays
    val allCampaigns = spark.read.parquet(campaignsPath)
      .filter($"sentTime" > date_add(lit(targetDateStr), -campaignDays.max))

    val campaignMaxSummary = allCampaigns
      .groupByAggregate(campaignGroupBy, campaignMaxAggs)

    val campaignCountSummary = canadaCampaignDays.map { daysBack =>
      allCampaigns
        .filterGroupByAggregate(getCampaignFilters(daysBack, targetDateStr), campaignGroupBy, getCampaignCountDaysAggs(daysBack))
    }

    val campaignSummary = (campaignMaxSummary +: campaignCountSummary)
      .joinAllSmall(Seq("email_id"), "left_outer")

    /**
     * Create a feature that counts the pending or unsuccessful attempts of a user to link bank account
     * and also state which bank those accounts belonged to.
     * Also mention if the user tried to do the same thing sometime in the same week so that we aren't
     * sending them campaign
     */

    // Read only a week's worth of logs
    val minus7DayPaths = for (i <- 0 to numOfDaysForBankLinks) yield {
      val dateStr = targetDate.minusDays(i).toString(logsFormatter)
      val path = s"${featuresDfs.heroLogs}/$dateStr"
      path
    }
    val dataDF = spark.read.json(minus7DayPaths: _*)

    val bankLinkAttempts = if (dataDF.columns.contains("provider_account_status")) {

      // Filter for rows containing json logs
      val cachedDF = dataDF
        .filter($"provider_account_status".isNotNull)
        .cache()

      // Convert to proper json(replace fat arrow with colon), Get status and provider values from json
      val statusDF = cachedDF
        .withColumn("provider_account_status", regexp_replace($"provider_account_status", "=>", ":"))
        .withColumn("status", get_json_object($"provider_account_status", "$.providerAccount.refreshInfo.status"))
        .withColumn("provider_id", get_json_object($"provider_account_status", "$.providerAccount.providerId"))
        .withColumnRenamed("@timestamp", "timestamp")
        .select("timestamp", "customer_id", "provider_id", "status")

      // Get latest status for each (customer, provider) combination, filter successes and get provider name
      val attemptsDF = statusDF.as[Record]
        .groupByKey(r => (r.customer_id, r.provider_id))
        .reduceGroups((x, y) => if (x.timestamp > y.timestamp) x else y)
        .map { case (c: (Long, String), r: Record) => (c._1, c._2, r.timestamp, r.status) }
        .toDF("customer_id", "provider_id", "timestamp", "latest_status")
        .filter(lower($"latest_status") =!= "success")
        .withColumn("date", substring($"timestamp", 0, 10))
        .join(bankProviders, Seq("provider_id"), defaultJoin)

      // Get num of days link attempted
      val countOfAttemptEachday = attemptsDF
        .groupBy("customer_id", "date", "provider_name")
        .count

      // Finally, aggregate by customer and convert list to comma seperated
      countOfAttemptEachday
        .groupByAggregate(byCustomer, bankLinkAggregations)
        .withColumn("bank_link_attempted_providers", seqToAndString($"bank_link_attempted_providers"))
    } else {
      val bankLinkSchema = StructType(
        StructField("customer_id", StringType, nullable = true) ::
          StructField("bank_link_attempted_providers", LongType, nullable = true) ::
          StructField("bank_link_attempted_last_7_days", StringType, nullable = true) :: Nil
      )
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bankLinkSchema)
    }

    /**
     * Create a feature to identify number of credit cards that are about to expire next month
     * with their last 4 digits
     */

    val expiringCC = paymentMethods
      .filter(getCreditCardCondition) // get only cc
      .dropDuplicates(Seq("brand", "last4")) // brand and last 4 digits would be good identifier of a cc
      .filter($"exp_year" === targetDate.getYear)
      .filter($"exp_month" === targetDate.plusMonths(1).getMonthOfYear)
      .groupByAggregate(byCustomer, expiringCCAggregations)
      .withColumn("cc_expiring_next_month", seqToAndString($"cc_expiring_next_month"))

    /**
     * Create a feature to save the date at which a customer's score crosses a particular value.
     * If a customer's score goes through this change more than one time, pick the latest date
     */

    val scoreIncresingFeature = scoreHistories
      .filter($"old_score" < scoreThreshold && $"new_score" >= scoreThreshold)
      .as[ScoreRecord]
      .groupByKey(r => r.customer_id)
      .reduceGroups((x, y) => if (x.created_at > y.created_at) x else y)
      .map { case (c: Long, r: ScoreRecord) => (c, r.created_at) }
      .toDF("customer_id", "created_at")
      .withColumn(s"score_increasing_${scoreThreshold}_date", $"created_at".cast(DateType))
      .drop("created_at")

    /**
     * Join all the above summary tables together
     */
    val allInfoJoinedPart1 = Seq(customerSummary, walletSummary, payeesSummary, ordersSummary,
      firstLastOrderSummary, giftCardSummary, billerSummary, billerCatSummary, createdSummary)
      .joinAllSmall(byCustomer, defaultJoin)
      .cache()
    allInfoJoinedPart1.count

    val allInfoJoinedPart2 = Seq(failedSummary, billerCreatedSummary, gcCreatedSummary, resurrectedSummary, walletBalanceSummary,
      paymentMethodSummary, allCategorySummary, loyaltyPoints, bankLinkAttempts, expiringCC, scoreIncresingFeature)
      .joinAllSmall(byCustomer, defaultJoin)
      .cache()
    allInfoJoinedPart2.count

    val allInfoJoined = Seq(allInfoJoinedPart1, allInfoJoinedPart2)
      .joinAllSmall(byCustomer, defaultJoin)
      .join(campaignSummary, Seq("email_id"), "left_outer")

    /**
     * Calculated Columns:
     * Calculate date difference features
     * Calculate division features
     * Calculate preferred payment types
     * Flag if users active last 35 days (not dormant) and resurrected, make columns relevant to segmentation 1-0,
     * then apply the customerSegment function to classify into segments
     * Fix data types, nulls, rounding, and drop irrelevant or date columns
     */
    val heroFeatures = allInfoJoined
      .withColumn("days_since_first_order", datediff(lit(targetDateStr), $"first_order_date"))
      .withColumn("days_since_first_bill_order", datediff(lit(targetDateStr), $"first_biller_order_date"))
      .withColumn("days_since_first_gc_order", datediff(lit(targetDateStr), $"first_gc_order_date"))
      .withColumn("days_since_last_order", datediff(lit(targetDateStr), $"latest_order_date"))
      .withColumn("days_to_first_order", datediff($"first_order_date", $"customer_created_at"))
      .withColumn("days_to_add_payment_method", datediff($"min_payment_added_time", $"customer_created_at"))
      .withColumn("avg_time_between_orders", $"days_since_first_order" / $"total_orders")
      .withColumn("avg_time_between_bill_orders", $"days_since_first_bill_order" / $"total_bill_orders")
      .withColumn("avg_time_between_gc_orders", $"days_since_first_gc_order" / $"total_gift_cards_orders")
      .withColumn("preferred_payment_type_orders", paymentPrefType($"count_wallet_orders", $"count_cc_orders", $"count_bank_orders"))
      .withColumn("preferred_payment_type_dollars", paymentPrefType($"total_spent_wallet", $"total_spent_cc", $"total_spent_bank"))
      .withColumn("active_last_35_days", when($"days_since_last_order" < 35, lit(1)).otherwise(0))
      .withColumn("resurrected_user", when(resurrectedCond, 1).otherwise(0))
      .na.fill(0, segmentNullZeroCols)
      .withColumn("customer_segment", customerSegment($"phone_verified", $"total_orders", $"count_payment_methods_added",
        $"email_verified", $"active_last_35_days", $"resurrected_user"))
      .withColumn("customer_id", $"customer_id".cast("long"))
      .dropAll(heroDropCols.head, heroDropCols.tail: _*)
      .transformListCols
      .roundDoubles
      .fillNulls
      .cache()

    /**
     * Drops deleted and blocked customers and writes directly to elastic search, overwriting the entire old index
     */
    heroFeatures
      .filter($"deleted" === 0 && $"blocked" === 0)
      .drop("deleted", "blocked")
      .saveEsSimple(heroFeaturesES.host, heroFeaturesES.port, heroFeaturesES.index, heroFeaturesES.indexType, "customer_id")

    spark.emptyDataFrame.write.mode(SaveMode.Overwrite).parquet(s"${featuresDfs.featuresTable}/$groupPrefix/temp")
  }
}

