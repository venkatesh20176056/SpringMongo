package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.SchemaRepo.L3RUSchema
import com.paytm.map.features.utils.ConvenientFrame.configureSparkForMumbaiS3Access
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object AggFeaturesL3RU extends AggFeaturesL3JobRU
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL3JobRU {
  this: SparkJob =>

  val JobName = "AggFeaturesL3RU"

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
    val aggL3RUPath = s"${baseDFS.aggPath}/L3RU"
    val anchorPath = s"${baseDFS.anchorPath}/L3RU"

    println(baseDFS.aggPath)

    import spark.implicits._

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Features for Cable and FastTag
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Unfiltered with Transaction status for last order transaction date feature
    val newSTRDataUnfiltered = spark.read.parquet(baseDFS.strMerchantPath)
      .addSTRRULevel()
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .repartition($"dt", $"customer_id")

    val salesTxnDataUnfiltered = newSTRDataUnfiltered.where($"txn_type" =!= 7)

    val salesTxnDataCashBackUnfiltered = newSTRDataUnfiltered.where($"txn_type" === 7)

    val newSTRTxnAggregatesUnfiltered = salesTxnDataUnfiltered
      .select($"customer_id", $"dt", $"RU_Level", $"str_id".as("order_item_id"), $"txn_amount".as("selling_price"), $"operator", $"created_at")
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level", isDiscount = false, unFiltered = true)
      .renameColsWithSuffix("_last_attempt_order_date", excludedColumns = Seq("customer_id", "dt"))

    val newSTRTxnPromoAggregatesUnfiltered = salesTxnDataCashBackUnfiltered
      .select($"customer_id", $"dt", $"RU_Level", $"str_id".as("order_item_id"), $"txn_amount".as("amount"))
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level")

    val finalDFSTRUnfiltered = newSTRTxnAggregatesUnfiltered
      .join(newSTRTxnPromoAggregatesUnfiltered, Seq("customer_id", "dt"), "left_outer")

    // Filtered with Transaction status
    val newSTRData = newSTRDataUnfiltered.where($"successfulTxnFlag" === 1)

    val salesTxnData = newSTRData.where($"txn_type" =!= 7)

    val salesTxnDataCashBack = newSTRData.where($"txn_type" === 7)

    val newSTRTxnAggregates = salesTxnData
      .select($"customer_id", $"dt", $"RU_Level", $"str_id".as("order_item_id"), $"txn_amount".as("selling_price"), $"operator", $"created_at")
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level", isDiscount = false)

    val newSTRTxnPromoAggregates = salesTxnDataCashBack
      .select($"customer_id", $"dt", $"RU_Level", $"str_id".as("order_item_id"), $"txn_amount".as("amount"))
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level")

    val finalDFSTR = newSTRTxnAggregates
      .join(newSTRTxnPromoAggregates, Seq("customer_id", "dt"), "left_outer")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Features for Cable and FastTag ENDS
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Unfiltered with Transaction status for last order transaction date feature
    val salesPromoRechUnfiltered = spark.read.parquet(baseDFS.salesPromRechPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .where($"isRechargeJoin".isNotNull) // implement inner join between Recharge and SO,SOI and fs_recharge
      .addRULevel()
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .repartition($"dt", $"customer_id")

    val salesAggregatesUnfiltered = salesPromoRechUnfiltered
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level", unFiltered = true)
      .renameColsWithSuffix("_last_attempt_order_date", excludedColumns = Seq("customer_id", "dt"))

    // Filtered with Transaction status
    val salesPromoRech = salesPromoRechUnfiltered
      .where($"successfulTxnFlag" === 1)

    // Generate Sales Aggregate
    val salesAggregates = salesPromoRech
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level")

    // Generate Sales First and Last Aggregate
    val firstLastTxn = salesPromoRech.select("customer_id", "dt", "RU_Level", "selling_price", "operator", "created_at")
      .union(
        salesTxnData
          .select($"customer_id", $"dt", $"RU_Level", $"txn_amount".as("selling_price"), $"operator", $"created_at")
      )
      .addFirstLastCol(Seq("customer_id", "dt", "RU_Level"), isOperator = true)
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level", isOperator = true)

    // Preferred Operator Count
    val preferableOperator = salesPromoRech
      .select("operator", "RU_Level", "customer_id", "dt")
      .union(
        salesTxnData
          .select("operator", "RU_Level", "customer_id", "dt")
      )
      .addPreferredOperatorFeat("RU_Level")

    // Generate Promocode Aggregates
    val promocodeAggregates = salesPromoRech
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level")

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    val dataDF = {
      salesAggregatesUnfiltered
        .join(finalDFSTRUnfiltered, Seq("customer_id", "dt"), "left_outer")
        .join(salesAggregates, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(preferableOperator, Seq("customer_id", "dt"), "left_outer")
        .join(promocodeAggregates, Seq("customer_id", "dt"), "left_outer")
        .join(finalDFSTR, Seq("customer_id", "dt"), "left_outer")
        .renameColumns(prefix = "L3_RU_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(L3RUSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }
    dataDF.moveHDFSData(dtSeq, aggL3RUPath)

  }

  private implicit class L3RUImplicits(dF: DataFrame) {

    def addSTRRULevel(): DataFrame = {
      import dF.sparkSession.implicits._
      dF.withColumn(
        "RU_Level",
        when(($"category" === "cable and broadband") && $"sub_category".like("%local cable operator%"), lit("CBL"))
          .when($"category" === "fastag", lit("FT"))
          .otherwise(lit(null))
      )
        .where($"RU_Level".isNotNull)
        .drop("category")
        .drop("sub_category")
    }

    def addRULevel(): DataFrame = {
      import dF.sparkSession.implicits._
      dF.withColumn(
        "RU_level",
        when($"service".like("%mobile%") && lower($"paytype").like("%prepaid%"), lit("MB_prepaid"))
          .when($"service".like("%mobile%") && lower($"paytype").like("%postpaid%"), lit("MB_postpaid"))
          .when($"service".like("%dth%"), lit("DTH"))
          .when($"service".like("%broadband%"), lit("BB"))
          .when($"service".like("%datacard%"), lit("DC"))
          .when($"service".like("%electricity%"), lit("EC"))
          .when($"service".like("%water%"), lit("WAT"))
          .when($"service".like("%gas%"), lit("GAS"))
          //.when($"service".like("%cable%"), lit("CBL"))
          .when($"service".like("%landline%"), lit("LDL"))
          .when($"service".like("%financial services%"), lit("INS"))
          .when($"service".like("%loan%"), lit("LOAN"))
          .when($"service".like("%metro%") && not(lower($"operator").like("%delhi metro%")), lit("MTC"))
          .when($"service".like("%metro%") && lower($"operator").like("%delhi metro%"), lit("MT"))
          .when($"service".like("%devotion%"), lit("DV"))
          .when($"service".like("%google play%"), lit("GP"))
          .when($"service".like("%challan%"), lit("CHL"))
          .when($"service".like("%municipal payments%"), lit("MNC"))
          .when($"service".like("%toll tag%"), lit("TOLL"))
          .when(lower($"paytype").like("%prepaid%"), lit("PREPAID"))
          .otherwise(lit(null))
      )
        .where($"RU_Level".isNotNull)
    }

  }

}