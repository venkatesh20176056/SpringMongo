package com.paytm.map.features.base

import java.sql.{Date, Timestamp}

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo.WalletSchema
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_set, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

object WalletFeatures extends WalletFeaturesJob
  with SparkJob with SparkJobBootstrap

case class subWalletBalIntermediate(
  customer_id: java.lang.Long,
  dt: Date,
  ppi_type: java.lang.String,
  sub_wallet_id: java.lang.Long,
  issuer_id: java.lang.Long,
  balance: java.lang.Double,
  create_timestamp: Timestamp
)

trait WalletFeaturesJob {
  this: SparkJob =>

  val JobName = "WalletFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggWalletPath = s"${baseDFS.aggPath}/WalletFeatures"
    val anchorPath = s"${baseDFS.anchorPath}/Wallet"

    import spark.implicits._
    // Make the DataFrame
    val newSTRTable = spark.read.parquet(baseDFS.strPath)

    lazy val corporateMerchant = DataTables.corporateMerchantsTable(spark, settings.datalakeDfs.corporate_merchants)
    lazy val walletUser = DataTables.walletUser(spark, settings.datalakeDfs.walletUser)
    lazy val walletDetails = DataTables.walletDetails(spark, settings.datalakeDfs.walletDetails)
    lazy val ppiDetails = DataTables.ppiDetailsTable(spark, settings.datalakeDfs.ppi_details)

    val PPItypeBytxntype = when($"txn_type" === 45, "FoodWallet")
      .when($"txn_type" === 46, "GiftWallet")
      .when($"txn_type" === 57, "FuelWallet")
      .when($"txn_type" === 56, "AllowanceWallet")

    // the join should happen with payer_id(its alias is customer_id as defined in newSTRTable)
    // and payer_id(its alias is customer_id as defined in newSTRTable) should be aliased to issuer_id
    // payee_id should be aliased here to customer_id
    //
    val lastBalanceCreditedDtIntermediate = newSTRTable
      .where($"txn_type".isin(45, 46, 56, 57))
      .where($"txn_status" === 1).as('newstr)
      .join(corporateMerchant.select($"mid").distinct().as('corpmerch), $"newstr.customer_id" === $"corpmerch.mid")
      .withColumn("ppi_type", PPItypeBytxntype)
      .select($"payee_id".alias("customer_id"), $"dt", $"ppi_type", $"customer_id".alias("issuer_id"), $"updated_date")
      .where($"ppi_type".isNotNull)
      .where($"issuer_id".isNotNull)
      .groupBy("customer_id", "dt", "ppi_type", "issuer_id")
      .agg(max("updated_date").alias("last_balance_credited_dt"))

    val lastBalanceCreditedDt = lastBalanceCreditedDtIntermediate
      .withColumn("lastBalanceCreditedDtStruct", struct(col("ppi_type"), col("issuer_id"), col("last_balance_credited_dt")))
      .groupBy("customer_id", "dt")
      .agg(
        collect_set("lastBalanceCreditedDtStruct").alias("SubwalletLastBalanceCreditedDt")
      )

    val PPItypeByppitype = when($"ppi_type" === 2, "FoodWallet")
      .when($"ppi_type" === 3, "GiftWallet")
      .when($"ppi_type" === 9, "FuelWallet")
      .when($"ppi_type" === 8, "AllowanceWallet")

    val walluser = walletUser.select($"customer_id", $"guid")
    val walldetail = walletDetails.select($"id", $"owner_guid")
    val ppidetails = ppiDetails.select($"id".as("sub_wallet_id"), $"balance", $"merchant_id", $"parent_wallet_id", $"dt", $"ppi_type", $"create_timestamp").withColumn("PPI_type", PPItypeByppitype)

    val lastCreateTimeWindow = Window.partitionBy($"customer_id", $"dt", $"ppi_type", $"issuer_id").orderBy($"create_timestamp".desc)
    val subwallbalintermediate = walluser
      .join(walldetail, $"guid" === $"owner_guid")
      .join(ppidetails, $"id" === $"parent_wallet_id")
      .select($"customer_id", $"dt", $"PPI_type".as("ppi_type"), $"sub_wallet_id", $"merchant_id".as("issuer_id"), $"balance", $"create_timestamp")
      .where($"ppi_type".isNotNull)
      .where($"issuer_id".isNotNull)
      .as[subWalletBalIntermediate]
      .select(
        $"customer_id",
        $"dt",
        $"ppi_type",
        $"issuer_id",
        first($"sub_wallet_id").over(lastCreateTimeWindow) as "sub_wallet_id",
        first($"balance").over(lastCreateTimeWindow) as "balance",
        $"create_timestamp"
      )

    val subwalletBal =
      subwallbalintermediate
        .withColumn("subWalletBalanceStruct", struct(col("ppi_type"), col("issuer_id"), col("balance"), col("dt")))
        .groupBy("customer_id", "dt")
        .agg(
          collect_set("subWalletBalanceStruct")
            .as("SubWalletBalance")
        )

    val walletLevel = when($"txn_type" === 5, "P2P")
      .when($"txn_type".isin(20, 4, 36), "A2W")
      .when($"txn_type".isin(53, 54), "A2W_AUTO")
      .otherwise(null)

    val customerId = when($"wallet_level" === "P2P", $"customer_id").otherwise($"payee_id")
    val isAddToWallet = $"wallet_level" === "A2W"
    val isAddToWalletAuto = $"wallet_level" === "A2W_AUTO"
    val possiblePaymentMethods = Seq("CC", "DC", "NB", "NEFT", "UPI").map("A2W_" + _)
    val possiblePaymentMethodsAuto = Seq("CC", "DC", "NB", "NEFT", "UPI").map("A2W_AUTO_" + _)

    val addToWalletPaymentMethod = when(isAddToWallet && $"byCC" === 1, "A2W_CC")
      .when(isAddToWallet && $"byDC" === 1, "A2W_DC")
      .when(isAddToWallet && $"byNB" === 1, "A2W_NB")
      .when(isAddToWallet && $"byNEFT" === 1, "A2W_NEFT")
      .when(isAddToWallet && $"byUPI" === 1, "A2W_UPI")
      .otherwise("A2W_Others")

    val addToWalletAutoPaymentMethod = when(isAddToWalletAuto && $"byCC" === 1, "A2W_AUTO_CC")
      .when(isAddToWalletAuto && $"byDC" === 1, "A2W_AUTO_DC")
      .when(isAddToWalletAuto && $"byNB" === 1, "A2W_AUTO_NB")
      .when(isAddToWalletAuto && $"byNEFT" === 1, "A2W_AUTO_NEFT")
      .when(isAddToWalletAuto && $"byUPI" === 1, "A2W_AUTO_UPI")
      .otherwise("A2W_AUTO_Others")

    val data = newSTRTable
      .where($"txn_status" === 1)
      .withColumn("wallet_level", walletLevel)
      .where($"wallet_level".isNotNull)
      .withColumn("A2W_paid_by", addToWalletPaymentMethod)
      .withColumn("A2W_AUTO_paid_by", addToWalletAutoPaymentMethod)
      .select(
        customerId.as("customer_id"),
        $"dt",
        $"wallet_level",
        $"str_id".as("order_item_id"),
        $"txn_amount".cast(DoubleType).as("selling_price"),
        $"created_at",
        $"payee_id",
        $"A2W_paid_by",
        $"A2W_AUTO_paid_by"
      )
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .repartition($"dt", $"customer_id")
      .cache

    // Calculate Aggregates
    val walletLevels = Seq("P2P", "A2W", "A2W_AUTO")
    val salesAggregates = data.addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "wallet_level", isDiscount = false, walletLevels)

    val paymentMethodCounts = data
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "A2W_paid_by", isDiscount = false, possiblePaymentMethods)

    val autoPaymentMethodCounts = data
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "A2W_AUTO_paid_by", isDiscount = false, possiblePaymentMethodsAuto)

    val firstLastTxn = data
      .addFirstLastCol(partitionCol = Seq("customer_id", "dt", "wallet_level"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "wallet_level", pivotValues = walletLevels)

    val firstLastTxnByPaymentMethod = data
      .addFirstLastCol(partitionCol = Seq("customer_id", "dt", "A2W_paid_by"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "A2W_paid_by", pivotValues = possiblePaymentMethods)

    val firstLastTxnByPaymentMethodAuto = data
      .addFirstLastCol(partitionCol = Seq("customer_id", "dt", "A2W_AUTO_paid_by"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "A2W_AUTO_paid_by", pivotValues = possiblePaymentMethodsAuto)

    val custIdCountFeatures = data
      .where($"wallet_level" === "P2P")
      .where($"payee_id".isNotNull)
      .groupBy("customer_id", "dt")
      .agg(
        collect_set($"payee_id").as("P2P_all_transaction_custID_list"),
        countDistinct($"payee_id").as("P2P_all_transaction_custID_count")
      )

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val finalColumns = WalletSchema.map(_.name).map(col)

    // Final Aggregates
    val dataDF = {
      subwalletBal
        .join(lastBalanceCreditedDt, Seq("customer_id", "dt"), "full_outer")
        .join(salesAggregates, Seq("customer_id", "dt"), "full_outer")
        .join(paymentMethodCounts, Seq("customer_id", "dt"), "left_outer")
        .join(autoPaymentMethodCounts, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastTxnByPaymentMethod, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastTxnByPaymentMethodAuto, Seq("customer_id", "dt"), "left_outer")
        .join(custIdCountFeatures, Seq("customer_id", "dt"), "left_outer")
        .renameColumns(prefix = "WALLET_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(WalletSchema)
        .select(finalColumns: _*) // this is to assure columns order remains same.
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }
    dataDF.moveHDFSData(dtSeq, aggWalletPath)
  }
}