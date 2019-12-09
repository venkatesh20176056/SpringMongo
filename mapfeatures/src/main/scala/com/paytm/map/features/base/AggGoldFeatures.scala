package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.base.DataTables._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import com.paytm.map.features.base.Constants._
import org.apache.spark.sql.types.DoubleType

object AggGoldFeatures extends AggGoldFeaturesJob
  with SparkJob with SparkJobBootstrap

trait AggGoldFeaturesJob {
  this: SparkJob =>

  val JobName = "AggGoldFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDate = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    //GA specific dates
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val startDateGA = targetDateGA.minusDays(gaBackFillDays)
    val dtGASeq = dayIterator(startDateGA, targetDateGA, ArgsUtils.formatter)

    import spark.implicits._

    val baseDFS = settings.featuresDfs.baseDFS
    val aggGoldPath = s"${baseDFS.aggPath}/GoldFeatures"
    val aggGoldGAPath = s"${baseDFS.aggPath}/GAFeatures/GOLD"
    val anchorPath = s"${baseDFS.anchorPath}/Gold"

    val buyOrders = DataTables.goldBuyOrders(spark, settings.datalakeDfs.goldBuyOrders, targetDateStr, startDate)
    val sellOrders = DataTables.goldSellOrders(spark, settings.datalakeDfs.goldSellOrders, targetDateStr, startDate)

    val redeem = DataTables.goldBackOrders(spark, settings.datalakeDfs.goldBuyOrders, targetDateStr, startDate)

    val aggBuy = buyOrders
      .addGoldTransactions("buy_")

    val aggSell = sellOrders
      .addGoldTransactions("sell_")

    val aggRedeem = redeem
      .select(
        $"dt",
        $"customer_id",
        $"amount"
      )
      .groupBy("dt", "customer_id")
      .agg(sum("amount").alias("total_goldback_received"))

    val firstLastBuy = buyOrders
      .select(
        $"customer_id",
        $"selling_price",
        $"dt",
        $"created_at"
      )
      .addFirstLastCol(Seq("customer_id", "dt"))
      .addFirstLastAggregates(Seq("customer_id", "dt"))
      .renameColumns(prefix = "buy_", excludedColumns = Seq("customer_id", "dt"))

    val firstLastSell = sellOrders
      .select(
        $"selling_price",
        $"customer_id",
        $"dt",
        $"created_at"
      )
      .addFirstLastCol(Seq("customer_id", "dt"))
      .addFirstLastAggregates(Seq("customer_id", "dt"))
      .renameColumns(prefix = "sell_", excludedColumns = Seq("customer_id", "dt"))

    val dataDf = {
      aggBuy
        .join(aggSell, Seq("customer_id", "dt"), "full_outer")
        .join(firstLastBuy, Seq("customer_id", "dt"), "left_outer")
        .join(firstLastSell, Seq("customer_id", "dt"), "left_outer")
        .join(aggRedeem, Seq("customer_id", "dt"), "full_outer")
        .renameColumns(prefix = "GOLD_", excludedColumns = Seq("customer_id", "dt"))
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDf.moveHDFSData(dtSeq, aggGoldPath)

    // Behavioural Feature
    val goldCustomEvent = lower($"event_category") === "digital_gold_home" &&
      lower($"event_action") === "home_screen_loaded"

    val pdp = DataTables.gaJoinedWebAppRaw(
      spark,
      settings.datalakeDfs.gaAppPaths,
      settings.datalakeDfs.gaWebPaths,
      settings.datalakeDfs.gaMallAppPaths,
      settings.datalakeDfs.gaMallWebPaths,
      targetDateGA, startDateGA,
      goldCustomEvent
    ).select(
      $"dt",
      $"customer_id",
      $"isApp",
      $"session_identifier"
    )
      .groupBy("customer_id", "dt")
      .agg(
        countDistinct(when($"isApp" === 1, $"session_identifier").otherwise(null)).as("clp_sessions_app"),
        countDistinct(when($"isApp" === 0, $"session_identifier").otherwise(null)).as("clp_sessions_web")
      )

    val goldGaAggregate = pdp
      .renameColumns(prefix = "GOLD_", excludedColumns = Seq("customer_id", "dt"))
      .coalesce(10)
      .cache

    goldGaAggregate.moveHDFSData(dtGASeq, aggGoldGAPath)

    // Gold Wallet Feature
    val goldWallet = DataTables.goldWallet(spark, settings.datalakeDfs.goldWallet, targetDateStr, startDate)
    val walletPath = s"${baseDFS.aggPath}/GoldWallet/dt=$targetDateStr"

    goldWallet.select(
      $"customer_id",
      lit("yes") as "is_gold_customer",
      $"gold_balance".cast(DoubleType).alias("GOLD_current_gold_balance")
    )
      .coalesce(10)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(walletPath)

  }

  implicit class goldFunctions(df: DataFrame) {
    def addGoldTransactions(prefix: String): DataFrame = {
      df
        .filter(col("status").equalTo(7))
        .addSalesAggregates(Seq("customer_id", "dt"), isDiscount = false)
        .renameColumns(prefix, excludedColumns = Seq("customer_id", "dt"))
    }
  }
}

