package com.paytm.map.features.datasets.hero

import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object HeroConstants {

  val createdDays = Seq(0, 1, 2)

  def getTimeSeriesDays: Seq[Int] = {
    Seq(30, 60, 90, 120, 150, 180, 730)
  }

  def getHeroKey: Seq[String] = {
    Seq("customer_id")
  }

  def getGroupByOrder: Seq[String] = {
    Seq("order_id")
  }

  def getDeletedFilter: Seq[String] = {
    Seq("deleted_at IS NULL")
  }

  def getWalletFilters: Seq[String] = {
    Seq("dst_account_type = 'EMONEY'")
  }

  def getOrderFilters: Seq[String] = {
    Seq("order_state NOT IN ('fulfillment_failed', 'created', 'payment_authorization_failed', 'refunded', " +
      "'manual_failed', 'expired', 'prerequisites_failed')")
  }

  def getPaymentsFilters: Seq[String] = {
    Seq("payments_state not in ('payment_authorization_failed', 'refunded', 'manual_failed', 'expired', " +
      "'prerequisites_failed')")
  }

  def getCreatedFilters: Seq[String] = {
    Seq("order_state = 'created'")
  }

  def getCompletedFilters: Seq[String] = {
    Seq("order_state not in ('created', 'expired')")
  }

  def getFailedFilters: Seq[String] = {
    Seq("order_state like '%failed%'")
  }

  /**
   * Makes related wallet-event data frame, takes walletEvents or wallets as input and renames all the columns
   */
  def getRelDf(walletDf: DataFrame): DataFrame = {
    val colNames = walletDf.schema.fieldNames
    val renamedColumns = colNames.map(name => col(name).as(s"rel_$name"))
    walletDf.select(renamedColumns: _*)
  }

  def getWalletNullZeroCols: Seq[String] = {
    Seq("sum_product_balance", "sum_product_current_balance")
  }

  def getWalletWindow: WindowSpec = {
    Window.partitionBy("customer_id").orderBy(desc("we_created_at"))
  }

  def getOrdersWindow: WindowSpec = {
    Window.partitionBy("customer_id").orderBy("order_created_at")
  }

  def getPaymentsCols: Seq[String] = {
    Seq("wallet_dollars", "pad_dollars", "cc_dollars")
  }

  def getSegmentNullZeroCols: Seq[String] = {
    Seq("phone_verified", "total_orders", "count_payment_methods_added", "email_verified",
      "active_last_35_days", "resurrected_user", "added_credit_card", "added_pad",
      "added_visa", "added_mastercard", "added_american_express")
  }

  def getResurrectedConditions: Column = {
    col("active_last_35_days") === 1 && col("max_time_between_orders") >= 35 && col("days_since_resurrection") < 35
  }

  def getOperatorConditions: Column = {
    col("operator").isNotNull
  }

  def getDropCols: Seq[String] = {
    Seq("email_verified_at", "customer_created_at", "kyc_verified_at", "hero_prod_email", "sign_in_email", "state",
      "min_phone_verified_at", "wallet_created_at", "min_payment_added_time", "latest_order_date",
      "first_order_date", "first_biller_order_date", "first_gc_order_date", "days_since_resurrection")
  }

  def getCreditCardCondition: Column = {
    lower(col("type")) like "%creditcard"
  }

  /**
   * Provides definitions for case-when helper columns that contain 1-0 flags or amounts to be used in aggregations,
   * Used in caseWhen function for each summary table
   * @param summaryTable Name of summary table as string requiring these conditions
   * @return Seq containing case-when rules
   */
  def getHeroConditions(summaryTable: String): Seq[(String, Column, Column)] = {

    summaryTable match {
      case "paymentMethod" => Seq(
        ("credit_card", getCreditCardCondition, lit(1)),
        ("pad", col("type") === "Pad", lit(1)),
        ("visa", col("brand") === "visa", lit(1)),
        ("mastercard", col("brand") === "mastercard", lit(1)),
        ("american_express", col("brand") === "american express", lit(1))
      )
      case "wallet" => Seq(
        ("p2p", col("src_account_type") === "EMONEY", col("dst_amount_dollars")),
        ("emt", col("campaign_token") === "EMT_GLOBAL", col("dst_amount_dollars")),
        ("can_post", col("campaign_token") === "CANADA_POST", col("dst_amount_dollars")),
        ("pad_topup", col("campaign_token") === "ONLINE_BANKING_TOPUP_GLOBAL", col("dst_amount_dollars")),
        ("customer_care", col("campaign_token") === "CUSTOMER_CARE", col("dst_amount_dollars"))
      )
      case "payees" => Seq(
        ("can_payees", col("payee_country_code") === "CA", col("payee_id")),
        ("ind_payees", col("payee_country_code") === "IN", col("payee_id"))
      )
      case "payments" => Seq(
        ("wallet_dollars", col("payments_type") === "WalletPayment", col("payments_amount")),
        ("pad_dollars", col("payments_type") like "%BankAccountPayment", col("payments_amount")),
        ("cc_dollars", col("payments_type") like "%CreditCardPayment", col("payments_amount"))
      )
      case "orders" => Seq(
        ("bill_time_of_day", col("biller_code").isNotNull, col("time_of_day")),
        ("bill_order_total", col("biller_code").isNotNull, col("order_total"))
      )
      case "firstLast" => Seq(
        ("biller_order_created_at", col("item_type") === "Payees::Base", col("order_created_at")),
        ("gc_order_created_at", col("item_type") === "Products::ProductVariant", col("order_created_at"))
      )
      case "billerCat" => Seq(
        ("biller_cat_credit_card", col("biller_category_name") === "Credit Cards", col("biller_category_id")),
        ("biller_cat_education", col("biller_category_name") === "Education", col("biller_category_id")),
        ("biller_cat_liabilities", col("biller_category_name") === "Liabilities", col("biller_category_id")),
        ("biller_cat_insurance", col("biller_category_name") === "Insurance", col("biller_category_id")),
        ("biller_cat_rent", col("biller_category_name") === "Rent", col("biller_category_id")),
        ("biller_cat_loans", col("biller_category_name") === "Loans", col("biller_category_id")),
        ("biller_cat_investments", col("biller_category_name") === "Investments", col("biller_category_id")),
        ("biller_cat_internet", col("biller_category_name") === "Internet", col("biller_category_id")),
        ("biller_cat_auto", col("biller_category_name") === "Auto", col("biller_category_id")),
        ("biller_cat_utilties", col("biller_category_name") === "Utilities", col("biller_category_id")),
        ("biller_cat_cable", col("biller_category_name") === "Cable", col("biller_category_id")),
        ("biller_cat_lifestyle", col("biller_category_name") === "Lifestyle", col("biller_category_id")),
        ("biller_cat_mobile", col("biller_category_name") === "Mobile", col("biller_category_id")),
        ("biller_cat_taxes", col("biller_category_name") === "Taxes", col("biller_category_id")),
        ("biller_cat_securities", col("biller_category_name") === "Securities", col("biller_category_id"))
      )
    }
  }

  /**
   * Sequences defining the aggregations to be used for each summary table with the filterGroupByAggregate function
   * @param summaryTable Name of summary table as string requiring these aggregations
   * @return Seq containing aggregation rules
   */
  def getHeroAggregations(summaryTable: String, conditions: Seq[(String, Column, Column)] = Seq()): Seq[(String, String)] = {

    summaryTable match {
      case "paymentMethod" =>
        conditions.map(pm => ("max", s"${pm._1}  as  added_${pm._1}")) ++
          Seq(
            ("count", "payment_method_id                    as  count_payment_methods_added"),
            ("sum", "pad                     as  count_bank_accounts_added"),
            ("sum", "credit_card             as  count_credit_cards_added"),
            ("min", "pm_created_at              as  min_payment_added_time"),
            ("collect_as_set", "provider_name   as  bank_account_providers"),
            ("collect_as_set", "brand           as  types_of_credit_cards_added")
          )
      case "wallet" =>
        Seq(
          ("sum", "dst_amount_dollars      as  total_wallet_txn_amount"),
          ("avg", "dst_amount_dollars      as  avg_wallet_txn_amount"),
          ("count", "txn_id                as  total_wallet_transactions")
        ) ++
          conditions.map(event => ("sum", s"${event._1} as  total_${event._1}_txn_amount")) ++
          conditions.map(event => ("avg", s"${event._1} as  avg_${event._1}_txn_amount")) ++
          conditions.map(event => ("count", s"${event._1} as  count_${event._1}_txns"))

      case "walletBalance" => Seq(
        ("min", "we_created_at            as  min_created_at"),
        ("sum", "product_balance          as  sum_product_balance"),
        ("sum", "product_current_balance  as  sum_product_current_balance")
      )
      case "payees" => Seq(
        ("count_distinct", "can_payees  as  num_canadian_payees_added"),
        ("count_distinct", "ind_payees  as  num_indian_payees_added")
      )
      case "payments" =>
        conditions.map(paymentType => ("sum", s"${paymentType._1} as order_${paymentType._1}"))
      case "orders" => Seq(
        ("count", "order_id               as  total_orders"),
        ("count", "payee_id               as  total_bill_orders"),
        ("count", "can_payees             as  total_canadian_bill_orders"),
        ("count", "ind_payees             as  total_indian_bill_orders"),
        ("count", "product_variant_id     as  total_gift_cards_orders"),
        ("sum", "order_total            as  total_spent"),
        ("avg", "order_total            as  avg_spend_per_order"),
        ("avg", "bill_order_total       as  avg_bill_spend_per_order"),
        ("avg", "product_variant_price  as  avg_gift_card_amount"),
        ("count", "promo_code             as  count_promo_codes_used"),
        ("sum", "promo_amount           as  sum_promo_cash_received"),
        ("count_distinct", "biller_name            as  num_billers_paid"),
        ("count_distinct", "biller_category_name     as  num_biller_categories_paid"),
        ("count", "order_wallet_dollars   as  count_wallet_orders"),
        ("count", "order_cc_dollars       as  count_cc_orders"),
        ("count", "order_pad_dollars     as  count_bank_orders"),
        ("sum", "order_wallet_dollars   as  total_spent_wallet"),
        ("sum", "order_cc_dollars       as  total_spent_cc"),
        ("sum", "order_pad_dollars     as  total_spent_bank"),
        ("avg", "time_of_day            as  avg_order_time"),
        ("avg", "bill_time_of_day       as  avg_bill_order_time")
      )
      case "firstLast" => Seq(
        ("max", "order_created_at          as  latest_order_date"),
        ("min", "order_created_at          as  first_order_date"),
        ("min", "biller_order_created_at   as  first_biller_order_date"),
        ("min", "gc_order_created_at       as  first_gc_order_date")
      )
      case "created" => Seq(
        ("count", "order_id    as  total_created_orders"),
        ("count", "canadian_biller as canadian_biller_created_orders"),
        ("count", "indian_biller as indian_biller_created_orders"),
        ("count", "gift_card as gift_card_created_orders")
      )
      case "failed" => Seq(
        ("count", "order_id    as  total_failed_orders")
      )
      case "billerCat" =>
        conditions.map(cat => ("count", s"${cat._1} as total_${cat._1}_orders"))
    }
  }

  /**
   * UDF to create 1-0 column indicating if customers have subscribed to email notifications in the app settings
   */
  val emailSubscribed: UserDefinedFunction = udf((notificationFlags: Integer) => {
    if (notificationFlags == 1 || notificationFlags == 3) 1
    else 0
  })

  /**
   * UDF to create 1-0 flag column indicating if given information for a customer has been entered
   */
  val infoEntered: UserDefinedFunction = udf((col: String) => {
    if (col == null) 0
    else 1
  })

  /**
   * UDF to create column showing the wallet transaction type with the most transactions out of email transfer,
   * Canada Post, and PAD top-up
   */
  val walletPrefType: UserDefinedFunction = udf((emt: Double, cp: Double, pad: Double) => {
    if (cp == 0 && pad == 0 && emt == 0) null
    else if (Seq(cp, pad, emt).max == cp) "can_post"
    else if (Seq(cp, pad, emt).max == pad) "pad"
    else "emt"
  })

  /**
   * UDF to create column showing the payment type with the most transactions by customer,
   * out of wallet, credit_card, and bank
   */
  val paymentPrefType = udf((wallet: Double, cc: Double, bank: Double) => {
    if (wallet == 0 && cc == 0 && bank == 0) null
    else if (Seq(wallet, cc, bank).max == wallet) "wallet"
    else if (Seq(wallet, cc, bank).max == bank) "bank"
    else "credit_card"
  })

  /**
   * UDF to create column with the customer segment. Customers who have not made any transactions are in created,
   * signup, or qualified depending on how much information is entered. Customers who have made 1 transaction are
   * new_active, multiple transactions are repeat. Customers who have made a transaction but not in the last 35 days
   * are dormant. Customers who have previously been dormant but are now active are resurrected. Once a resurrected
   * user has been resurrected for 35 days they become repeat.
   */
  val customerSegment = udf((phoneVerified: Integer, totalOrders: Integer, countPaymentMethods: Integer,
    emailVerified: Integer, activeLast35Days: Integer, resurrected: Integer) => {
    if (phoneVerified == 0 && totalOrders == 0) "created"
    else if ((phoneVerified == 1 && countPaymentMethods == 0 && totalOrders == 0) ||
      (emailVerified == 0 && countPaymentMethods > 0 && totalOrders == 0)) "signup"
    else if (phoneVerified == 1 && emailVerified == 1 && totalOrders == 0 && countPaymentMethods > 0) "qualified"
    else if (totalOrders > 0 && activeLast35Days == 0) "dormant"
    else if (totalOrders == 1 && activeLast35Days == 1 && resurrected == 0) "new_active"
    else if (totalOrders > 1 && activeLast35Days == 1 && resurrected == 0) "repeat"
    else if (totalOrders > 0 && resurrected == 1) "resurrected"
    else "other"
  })

  /**
   * Gets product specific filters for gift card and biller product aggregations
   * Used in getPurchasePrefSummary
   */
  def getHeroProductFilters(productName: String, orderFilters: Seq[String]): Seq[String] = {
    productName match {
      case "giftCard" =>
        orderFilters ++ Seq("product_variant_id IS NOT NULL")
      case _ =>
        orderFilters ++ Seq("payee_id IS NOT NULL")
    }
  }

  /**
   * For use in getting most recent date of a biller / gift card order, specifies the date field output
   * from the firstLast condition
   */
  def getHeroProductDate(productName: String): String = {
    productName match {
      case "giftCard" =>
        "gc_order_created_at"
      case "biller" =>
        "biller_order_created_at"
      case _ =>
        s"${productName}_order_created_at"
    }
  }

  /**
   * Get feature names for most recent purchases and the field to choose in the getFirstLast function
   */
  def getHeroRecentFieldAs(productName: String): Seq[String] = {
    productName match {
      case "giftCard" =>
        Seq("gift_card_name_amount as most_recent_gift_card_purchase")
      case "biller" =>
        Seq("biller_name as most_recent_biller_paid")
      case _ =>
        Seq(s"$productName as most_recent_${productName}_purchase")
    }
  }

  /**
   * Defines field name for product aggregations
   * Used in getPurchasePrefSummary
   */
  def getHeroProductFieldName(productName: String): String = {
    productName match {
      case "giftCard"  => "gift_card_name_amount"
      case "biller"    => "biller_name"
      case "billerCat" => "biller_category_name"
    }
  }

  /**
   * Gets aggregation rules for each product type
   * Used in getPurchasePrefSummary
   */
  def getHeroPreAgg(productName: String): Seq[(String, String)] = {
    productName match {
      case "giftCard" => Seq(
        ("count", "product_variant_id     as  count")
      )
      case _ => Seq(
        ("count", "payee_id               as  count")
      )
    }
  }

  /**
   * Gets aggregation rules used in created biller / gc list features
   */
  val preAggCreated = Seq(("count", "order_id   as  created_count"))
  val preAggCompleted = Seq(("count", "order_id   as  completed_count"))

  /**
   * Gets rules for selecting used in getFirstLast window function
   * Used in getPurchasePrefSummary
   */
  def getHeroProductFirstLast(productName: String): Seq[String] = {
    productName match {
      case "giftCard" => Seq(
        "gift_card_name_amount         as  preferred_gift_card"
      )
      case "biller" => Seq(
        "biller_name                   as  preferred_biller"
      )
      case "billerCat" => Seq(
        "biller_category_name          as  preferred_biller_category"
      )
    }
  }

  /**
   * UDF to convert
   * Seq(a,b,c) to "a, b and c"
   * Seq(a,b) to "a and b"
   * Seq(a) to "a"
   * Seq() to null
   */

  val seqToAndString: UserDefinedFunction = udf((col: Seq[String]) => {
    col.size match {
      case 0     => null
      case 1 | 2 => col.mkString(" and ")
      case _ =>
        val lastElement = col.last
        val allExceptLast = col.dropRight(1).mkString(", ")
        allExceptLast + " and " + lastElement
    }
  })

  /**
   * UDF helper function getPurchasePrefSummary that returns a dataframe with column containgin a string json-map
   * of names of billers or gift cards and the count of each that a customer has purchased
   * @param ordersDf Dataframe grouped by order
   * @param filters Order and product specific filters to apply to the order data frame
   * @param fieldName Column name of biller of gift card variant
   * @param preAgg Rules defining aggregation to take count of each product variant per customer
   * @param custField Customer field
   * @param days Look back days for time series analysis
   * @return Data frame with 4 columns: customer_id, most purchased gift card / biller, list of all gift cards or
   *         billers paid, list of all gift cards or billers paid multiple times
   */
  def getPurchasePrefSummary(
    ordersDf: DataFrame,
    filters: Seq[String],
    fieldName: String,
    preAgg: Seq[(String, String)],
    custField: Seq[String],
    days: Int
  ): DataFrame = {

    val purchaseCatGroupBy = custField :+ fieldName

    val catAgg = Seq(
      ("collect_as_set", s"$fieldName as ${fieldName}_paid_${days}_days")
    )

    val catAggMultiple = Seq(
      ("collect_as_set", s"$fieldName as ${fieldName}_paid_multiple_times_${days}_days")
    )

    val firstLastName = Seq(s"$fieldName as preferred_${fieldName}_${days}_days")

    val purchaseCat = ordersDf
      .filterGroupByAggregate(filters, purchaseCatGroupBy, preAgg)

    val preferred = purchaseCat
      .getFirstLast(custField, "count", firstLastName, isFirst = false)

    val paidOnceList = ordersDf
      .filterGroupByAggregate(filters, custField, catAgg)

    val paidMultipleList = purchaseCat
      .filterGroupByAggregate(Seq("count > 1"), custField, catAggMultiple)

    preferred
      .join(paidOnceList, custField, "left_outer")
      .join(paidMultipleList, custField, "left_outer")
  }

  /**
   * Aggregation rules for getting time between orders after using window function
   */
  def getOrdersWindowAgg: Seq[(String, String)] = {
    Seq(
      ("max", "time_between_orders       as  max_time_between_orders"),
      ("min", "days_since_resurrection   as  days_since_resurrection")
    )
  }

  /**
   * Aggregation rules for bank link attempts
   */
  def getBankLinkAggregations: Seq[(String, String)] = {
    Seq(
      ("collect_as_list", "provider_name as  bank_link_attempted_providers"),
      ("sum", "count         as  bank_link_attempted_last_7_days")
    )
  }

  /**
   * Aggregation rules for getting num of expiring credit cards and thier last 4 digits
   */
  def getExpiringCCAggregations: Seq[(String, String)] = {
    Seq(
      ("count", "customer_id as  num_of_cc_expiring_next_month"),
      ("collect_as_list", "last4         as  cc_expiring_next_month")
    )
  }

}

