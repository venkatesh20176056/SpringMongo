package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.SchemaRepo.PaymentsSchema
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object PaymentFeatures extends PaymentFeaturesJob
  with SparkJob with SparkJobBootstrap

trait PaymentFeaturesJob {
  this: SparkJob =>

  val JobName = "PaymentFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    import spark.implicits._
    // make path
    val baseDFS = settings.featuresDfs.baseDFS
    val aggPaymentsPath = s"${baseDFS.aggPath}/PaymentFeatures"

    //Read Data
    val offlineTxnData = spark.read.parquet(baseDFS.paymentsBasePath)
      .drop("alipay_trans_id") // dropping this col since it wasnt present here, this col was just added for avoiding datalake and features join in txnbase in measurements
      .where($"customer_id".isNotNull)
      .where($"merchant_L1_type" === "OFFLINE")
      .withColumn("merchant_category", trim(lower($"merchant_category".cast(StringType))))
      .withColumn("merchant_subcategory", trim(lower($"merchant_subcategory".cast(StringType))))
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .repartition($"customer_id", $"dt")
      .cache()

    // offline Overall
    val offlineOverall = offlineTxnData
      .appendFeatures("merchant_L1_type", lit(true), "", "merchant_L2_type", "merch_type", pivotValues = Seq("OFFLINE"), isMIDCol = true)

    // offline Payments
    val offlinePayments = offlineTxnData
      .appendFeatures("payment_type", lit(true), "OFFLINE_", pivotValues = Seq("MIDP", "P2P"))

    val offlinePaymentsByCategory = offlineTxnData
      .withColumn("merchant_category_upper", regexp_replace(upper($"merchant_category".cast(StringType)), "\\s", ""))
      .appendFeatures("merchant_category_upper", lit(true), "OFFLINE_", pivotValues = Seq("FOOD", "GASANDPETROL"))

    //unOrg Overall
    val unOrgOverall = offlineTxnData
      .appendFeatures("merchant_L2_type", $"merchant_L2_type" === "UNORG", "OFFLINE_", pivotValues = Seq("UNORG"), isMIDCol = true)

    //unOrg Payments
    val unOrgPayments = offlineTxnData
      .appendFeatures("payment_type", $"merchant_L2_type" === "UNORG", "OFFLINE_UNORG_", pivotValues = Seq("MIDP", "P2P"))

    //DIY Overall
    val diyOverall = offlineTxnData
      .appendFeatures("merchant_L2_type", $"merchant_L2_type" === "DIY", "OFFLINE_", pivotValues = Seq("DIY"), isMIDCol = true)

    //DIY Payments
    val diyPayments = offlineTxnData
      .appendFeatures("payment_type", $"merchant_L2_type" === "DIY", "OFFLINE_DIY_", pivotValues = Seq("MIDP", "P2P"))

    //KAM Overall
    val kamOverall = offlineTxnData
      .appendFeatures("merchant_L2_type", $"merchant_L2_type" === "KAM", "OFFLINE_", "merchant_category", "cat_type", pivotValues = Seq("KAM"), isMIDCol = true)

    //Transaction features with category
    val offKamData = offlineTxnData.addTxnsByCategoryFeatures($"merchant_L2_type" === "KAM", "OFFLINE_KAM_")
    val offUnOrgData = offlineTxnData.addTxnsByCategoryFeatures($"merchant_L2_type" === "UNORG", "OFFLINE_UNORG_")
    val offAllData = offlineTxnData.addTxnsByCategoryFeatures(lit(true), "OFFLINE_ALL_")

    // UPI txns to Offline merchant
    val offUPIData = offlineTxnData.appendFeatures(filter = $"byUPI" === 1, prefix = "OFFLINE_UPI_", pivotCol = "payment_type", pivotValues = Seq("MIDP"))

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    offlineOverall
      .join(offlinePayments, Seq("customer_id", "dt"), "outer")
      .join(offlinePaymentsByCategory, Seq("customer_id", "dt"), "outer")
      .join(unOrgOverall, Seq("customer_id", "dt"), "outer")
      .join(unOrgPayments, Seq("customer_id", "dt"), "outer")
      .join(diyOverall, Seq("customer_id", "dt"), "outer")
      .join(diyPayments, Seq("customer_id", "dt"), "outer")
      .join(kamOverall, Seq("customer_id", "dt"), "outer")
      .join(offKamData, Seq("customer_id", "dt"), "outer")
      .join(offUnOrgData, Seq("customer_id", "dt"), "outer")
      .join(offAllData, Seq("customer_id", "dt"), "outer")
      .join(offUPIData, Seq("customer_id", "dt"), "outer")
      .alignSchema(PaymentsSchema)
      .coalescePartitions("dt", "customer_id", dtSeq)
      .write.partitionBy("dt")
      .mode(SaveMode.Overwrite)
      .parquet(aggPaymentsPath)
  }

  private implicit class PaymentsImplicits(dF: DataFrame) {
    import dF.sparkSession.implicits._

    def appendFeatures(pivotCol: String, filter: Column, prefix: String, operatorCol: String = "", operatorStr: String = "", pivotValues: Seq[String] = Seq(), isMIDCol: Boolean = false): DataFrame = {
      import com.paytm.map.features.base.DataTables.DFCommons

      val data = dF.where(filter)
      //Add Sales Features
      val salesFeat = data
        .addSalesAggregates(Seq("customer_id", "dt"), pivotCol, pivotValues = pivotValues, isDiscount = false)

      //Add FirstLast Features
      val firstLastData = if (operatorCol == "") data else data.withColumn("operator", col(operatorCol))
      val isOperator = if (operatorCol == "") false else true
      val firstLastFeat = firstLastData
        .addFirstLastCol(Seq("customer_id", "dt", pivotCol), isOperator)
        .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol, isOperator, pivotValues)
        .renameOperatorCol(operatorStr)

      //mid columns
      val aggCols = Seq(
        countDistinct(col("mid")).as("total_txn_MID_count"),
        collect_set(col("mid")).as("all_txn_MID_list")
      )
      val midData = data
        .groupPivotAgg(Seq("customer_id", "dt"), pivotCol, aggCols, pivotValues = pivotValues)

      // Join Features
      val nonMidJoined = salesFeat.join(firstLastFeat, Seq("customer_id", "dt"), "inner")
      val joinedData = if (isMIDCol) nonMidJoined.join(midData, Seq("customer_id", "dt"), "inner") else nonMidJoined
      joinedData.renameColumns(prefix, excludedColumns = Seq("customer_id", "dt"))

    }

    def renameOperatorCol(operatorStr: String): DataFrame = {
      val newColNames = dF.columns.map { colM =>
        if (colM.endsWith("_transaction_operator")) colM.replace("operator", operatorStr) else colM
      }
      dF.toDF(newColNames: _*)
    }

    def addTxnsByCategoryFeatures(filterCondition: Column, prefix: String): DataFrame = {
      dF
        .filter(filterCondition)
        .groupBy($"customer_id", $"dt", $"merchant_category", $"merchant_subcategory")
        .agg(
          count(lit(1L)) as "count",
          sum($"selling_price") as "amount"
        )
        .withColumn(
          "TXN",
          struct(
            $"merchant_category".as("category"),
            $"merchant_subcategory".as("subCategory"),
            $"count",
            $"amount"
          )
        )
        .groupBy($"customer_id", $"dt")
        .agg(
          collect_list($"TXN").as("TXNS")
        )
        .select($"customer_id", $"dt", $"TXNS")
        .renameColumns(prefix, excludedColumns = Seq("customer_id", "dt"))
    }
  }

}