package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.base.DataTables._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.joda.time.format.DateTimeFormat

object AggFeaturesPostPaid extends AggFeaturesPostPaidJob
    with SparkJob with SparkJobBootstrap {

}

trait AggFeaturesPostPaidJob {
  this: SparkJob =>

  val JobName = "AggPostpaid"

  val FormatPatternMonthYear = "MM-yyyy"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val currentMonthYear = DateTimeFormat.forPattern(FormatPatternMonthYear).print(targetDate)

    val lookBackDays: Int = args(1).toInt

    val startDate = targetDate.minusDays(lookBackDays)

    val startDateStr = startDate.toString(ArgsUtils.formatter)

    val leads = DataTables.leadsTable(spark, settings.datalakeDfs.leadsTable)
    val accounts = DataTables.digitalCreditAccounts(spark, settings.datalakeDfs.digitalCreditAccounts)

    val eligible_users = DataTables.digitalCreditEligibleUsers(spark, settings.datalakeDfs.digitalCreditEligibleUsers)

    val early_access = DataTables.digitalCreditEarlyAccess(spark, settings.datalakeDfs.digitalCreditEarlyAccess)
    val bills = DataTables.digitalCreditBills(spark, settings.datalakeDfs.digitalCreditBills)
    val statement = DataTables.digitalCreditStatement(spark, settings.datalakeDfs.digitalCreditStatements)
    val bankInitiatedTxn = DataTables.digitalCreditBankInitiated(spark, settings.datalakeDfs.digitalCreditBankInitiated)
    val revolve = DataTables.digitalCreditRevolve(spark, settings.datalakeDfs.digitalCreditRevolve)
    val subscriptions = DataTables.digitalCreditSubscriptions(spark, settings.datalakeDfs.digitalCreditSubscriptions)

    val acquisitionModel = DataTables.postPaidAcquisitionModel(spark, settings.datalakeDfs.postPaidAcquisitionModelV2)
    val postPaidAdoption = DataTables.postPaidAdoption(spark, settings.datalakeDfs.postPaidAdoptionScore)
    val deferEligible = DataTables.postPaidDeferEligible(spark, settings.datalakeDfs.postPaidDeferEligible)

    val baseDFS = settings.featuresDfs.baseDFS
    val outPath = s"${baseDFS.aggPath}/PostPaid/dt=$targetDateStr/"
    val outPathDate = s"${baseDFS.aggPath}/PostPaid_Date/"

    //Onboarding Features
    val reqInvite = leads
      .withColumn("request_invite_flag", lit(1))
      .select("customer_id", "request_invite_flag")

    val accountsCustomer = accounts.select($"customer_id", $"account_number")

    val accountsStatusFeatures = accounts
      .select($"customer_id", $"account_status", $"application_status")

    val accountsFeatures = accounts
      .filter($"application_status" === 6)
      .withColumn("live_status", lit(1))
      .withColumnRenamed("authorised_credit_limit", "spend_limit")
      .withColumn("limit_utilization", lit(100) - ($"available_credit_limit" / $"spend_limit") * lit(100))
      .withColumn("total_outstanding_amount", $"spend_limit" - $"available_credit_limit")
      .withColumn("defer_status", when($"extended_due_date".isNotNull, lit(1)).otherwise(0))
      .withColumn("bill_unpaid", when($"due_amount".gt(0), lit(1)).otherwise(0))
      .withColumn("bill_paid", when($"due_amount".lt(1), lit(1)).otherwise(0))
      .withColumn("defaulter_flag", when($"account_status" === 1, lit(1)).otherwise(0))
      .withColumn("due_date", when($"account_status".isin(1, 2) && $"due_amount" >= 1, to_date($"due_date")).otherwise(lit(null)))
      .select("customer_id", "live_status", "spend_limit", "available_credit_limit",
        "limit_utilization", "total_outstanding_amount", "defer_status", "bill_unpaid",
        "bill_paid", "defaulter_flag", "due_amount", "due_date")

    val subscriptionFeatures = subscriptions
      .withColumn("si_enabled_status", when($"subscription_status" === 2, lit(1)).otherwise(0))
      .join(accountsCustomer, Seq("account_number"))
      .select($"customer_id", $"si_enabled_status")

    val earlyAccessFeature = early_access
      .withColumn("early_access_flag", lit(1))
      .select("customer_id", "early_access_flag")

    val whiteListedFeature = eligible_users
      .withColumn("whitelisted_flag", lit(1))
      .withColumn("product_type", when($"product_type" === 0, "Regular").otherwise("Chhota"))
      .select($"customer_id", $"whitelisted_flag", to_date($"go_live_date").alias("go_live_date"), $"product_type")

    val billPayments = bills
      .filter($"month_year" === currentMonthYear)
      .withColumn("bill_generated_flag", when($"total_amt".gt(0), lit(1)).otherwise(0))
      .join(accountsCustomer, Seq("account_number"))
      .select($"customer_id", $"total_amt" as "total_amount", $"bill_generated_flag")

    val lastTransaction = statement
      .join(accounts, Seq("account_number"))
      .groupBy("customer_id")
      .agg(max(to_date($"updated_at")).alias("last_transaction_date"))

    val finalDf = reqInvite
      .join(accountsFeatures, Seq("customer_id"), "full_outer")
      .join(accountsStatusFeatures, Seq("customer_id"), "full_outer")
      .join(earlyAccessFeature, Seq("customer_id"), "full_outer")
      .join(whiteListedFeature, Seq("customer_id"), "full_outer")
      .join(subscriptionFeatures, Seq("customer_id"), "left_outer")
      .join(billPayments, Seq("customer_id"), "left_outer")
      .join(lastTransaction, Seq("customer_id"), "left_outer")
      .join(acquisitionModel, Seq("customer_id"), "full_outer")
      .join(postPaidAdoption, Seq("customer_id"), "full_outer")
      .join(deferEligible, Seq("customer_id"), "full_outer")
      .addPrefixToColumns("POSTPAID_", Seq("customer_id"))

    finalDf.write.mode(SaveMode.Overwrite).parquet(outPath)

    //Date partitioned features

    val dtSeq = dayIterator(targetDate.minusDays(30), targetDate, ArgsUtils.formatter)

    val topMID = topMerchantFeatures(spark, settings, targetDateStr, startDateStr)

    val pgOlap = DataTables.getPGAlipayOLAP(spark, settings.datalakeDfs.alipayPgOlap, targetDateStr, startDateStr)
      .where($"customer_id".isNotNull)

    val lateFee = bankInitiatedTxn
      .join(accounts, Seq("account_number"))
      .groupBy($"customer_id", $"dt")
      .agg(sum("amount").alias("late_fee_amount"))

    val defer = revolve
      .join(accounts, Seq("account_number"))
      .groupBy("dt", "customer_id")
      .agg(count("*").alias("defers"))

    val dropOff = pgOlap
      .filter($"responsecode" isin (401, 810))
      .groupBy("customer_id", "dt")
      .agg(count("*").alias("pg_drop_off_count"))

    defer
      .join(dropOff, Seq("customer_id", "dt"), "full_outer")
      .join(topMID, Seq("customer_id", "dt"), "full_outer")
      .join(lateFee, Seq("customer_id", "dt"), "full_outer")
      .addPrefixToColumns("POSTPAID_", Seq("customer_id", "dt"))
      .coalescePartitions("dt", "customer_id", dtSeq)
      .cache
      .moveHDFSData(dtSeq, outPathDate)
  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def topMerchantFeatures(spark: SparkSession, settings: Settings, targetDate: String, startDate: String): DataFrame = {
    import spark.implicits._

    val merchantData = DataTables.merchantTable(spark, settings.datalakeDfs.merchantUser)
      .select(
        $"merchant_id" as "payee_id",
        $"operator".alias("display_name"),
        $"pg_mid".alias("mid")
      )

    spark.read.parquet(settings.featuresDfs.baseDFS.strPath)
      .filter($"dt" > startDate)
      .select(
        $"customer_id",
        $"payee_id",
        $"txn_amount".cast(DoubleType) as "txn_amount",
        $"dt",
        $"txn_type",
        $"merchant_order_id",
        $"mode",
        $"metadata"
      )
      .withColumn(
        "type",
        when($"mode" === "QR_CODE", "scan_pay_out")
          .otherwise(when($"txn_type".isin(1, 5, 69) && substring($"merchant_order_id", 1, 2) === "QR", "scan_pay_out")
            .otherwise(when($"mode" === "TOTP" || $"metadata" === "POS", "totp_out")))
      )
      .groupPivotAgg(Seq("customer_id", "payee_id", "dt"), "type", Seq(count("*").alias("txn_count")), pivotValues = Seq("totp_out", "scan_pay_out"))
      .join(merchantData, Seq("payee_id"), "inner")
      .drop("payee_id")
      .withColumn("totp_out", when($"totp_out" > 3, $"totp_out").otherwise(0))
      .withColumn("scan_pay_out", when($"scan_pay_out" > 3, $"scan_pay_out").otherwise(0))
      .filter("totp_out > 0 or scan_pay_out > 0")
      .withColumn("merch_struct", struct($"display_name", $"totp_out", $"scan_pay_out"))
      .groupBy("customer_id", "dt").agg(collect_set($"merch_struct").alias("merch_struct"))
  }

}