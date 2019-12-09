package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.functions.{col, when, _}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SalesBaseJob extends SparkJob with SparkJobBootstrap with SalesBase

trait SalesBase {
  this: SparkJob =>

  val JobName = "MerchantTransactions"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    implicit val imspark = spark
    import spark.implicits._

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val lookBackDays: Int = args(1).toInt

    val inputDirectory = settings.featuresDfs.baseDFS.paymentsBasePath

    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    val aggPath = settings.featuresDfs.baseDFS.aggPath
    val outputPath = s"$aggPath/MerchantSales"

    val baseTable = spark.read.parquet(inputDirectory)

    val filteredInput = getQuery(baseTable, settings)
      .where($"dt".between(dtSeq.head, dtSeq.last))
      .repartition(20, groupByColumns.map(col): _*)
      .cache

    val output = overallAgg(filteredInput)
      .join(paymentTypeAgg(filteredInput), groupByColumns, "outer")
      .join(edcRequestTypeAgg(filteredInput), groupByColumns, "outer")
      .renameColumns("p2m_", groupByColumns)
      .cache

    output.moveHDFSData(dtSeq, outputPath)
  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def getQuery(inputDF: DataFrame, settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    inputDF.printSchema()

    inputDF
      .filter($"payment_mode" =!= "BALANCE" or $"payment_mode".isNull)
      .filter($"successfulTxnFlag" === 1)
      .select(
        $"customer_id",
        $"mid".alias("merchant_id"),
        $"order_item_id",
        $"selling_price",
        $"created_at",
        $"source",
        $"dt",
        when($"byUPI" === 1, "upi").otherwise(
          when($"byNB" === 1, "netbanking")
            .otherwise("other")
        ).alias("level"),
        lower($"payment_type").as("payment_type")
      )
  }

  def aggFunctions: Seq[Column] = {
    Seq(
      count(col("order_item_id")).as("total_transaction_count"),
      coalesce(sum(col("selling_price")), lit(0)).as("total_transaction_size"),
      min(to_date(col("created_at"))).as("first_transaction_date"),
      max(to_date(col("created_at"))).as("last_transaction_date")
    )
  }

  val groupByColumns = Seq("merchant_id", "dt", "customer_id")

  def pivotValues: Seq[String] = Seq("upi", "netbanking", "other")

  def overallAgg(df: DataFrame): DataFrame = {
    df
      .where(col("customer_id").isNotNull)
      .groupPivotAgg(groupByColumns, "level", aggFunctions, pivotValues = pivotValues)
      .select(
        col("*"),
        (col("upi_total_transaction_count") + col("netbanking_total_transaction_count") + col("other_total_transaction_count")).alias("overall_total_transaction_count"),
        (col("upi_total_transaction_size") + col("netbanking_total_transaction_size") + col("other_total_transaction_size")).alias("overall_total_transaction_size"),
        (when(col("upi_first_transaction_date").lt("netbanking_first_transaction_date"), col("upi_first_transaction_date")).otherwise(col("netbanking_first_transaction_date"))).alias("overall_first_transaction_date"),
        (when(col("upi_last_transaction_date").gt("netbanking_last_transaction_date"), col("upi_last_transaction_date")).otherwise(col("netbanking_last_transaction_date"))).alias("overall_last_transaction_date")
      )
  }

  def paymentTypeAgg(newSTR: DataFrame): DataFrame = {
    import newSTR.sparkSession.implicits._

    newSTR
      .where($"customer_id".isNotNull)
      .groupPivotAgg(groupByColumns, "payment_type", aggFunctions, pivotValues = Seq("midp", "p2p"))
      .select(
        $"merchant_id",
        $"customer_id",
        $"dt",
        $"midp_total_transaction_count",
        $"p2p_total_transaction_count"
      )
  }

  def edcRequestTypeAgg(paymentsData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val aggFuncs = Seq(
      count(col("order_item_id")).as("edc_total_transaction_count"),
      coalesce(sum(col("selling_price")), lit(0)).as("edc_total_transaction_size")
    )

    paymentsData
      .where($"request_type".isin("EDC", "DYNAMIC_QR"))
      .groupBy(groupByColumns.head, groupByColumns.tail: _*)
      .agg(aggFuncs.head, aggFuncs.tail: _*)
      .select(
        $"merchant_id",
        $"customer_id",
        $"dt",
        $"edc_total_transaction_count",
        $"edc_total_transaction_size"
      )
  }
}
