package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.AggFeaturesL2.L2Implicits
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.Constants._
import com.paytm.map.features.base.DataTables.{DFCommons, getOnPaytmMID, getSalesAndWallet}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

object AggFeaturesL1 extends AggFeaturesL1Job
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL1Job {
  this: SparkJob =>

  val JobName = "AggFeaturesL1"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    // Get the command line parameters

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDate = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    //GA specific dates
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val dtGASeq = dayIterator(targetDateGA.minusDays(gaBackFillDays), targetDateGA, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggL1Path = s"${baseDFS.aggPath}/L1"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val anchorPath = s"${baseDFS.anchorPath}/L1"

    // Get Mappings
    val onPaytmMID: Array[String] = getOnPaytmMID(spark, settings, targetDateStr)
    val verticalMap = DataTables.catalogCategoryTable(spark, settings.datalakeDfs.catalog_category).getCCMap(isCategoryKey = false)

    // Get Data
    val salesTable = spark.read.parquet(baseDFS.salesDataPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .addL2LevelCol(verticalMap, isCategoryKey = false)
      .applySuccessfulFilter
    val newSTRTable = spark.read.parquet(baseDFS.strPath).getWalletTxns(onPaytmMID)
    val salesAndWalletTable = getSalesAndWallet(salesTable, newSTRTable)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .repartition($"dt", $"customer_id")
      .cache

    val salesAggregates = salesAndWalletTable
      .select("customer_id", "dt", "order_item_id", "selling_price", "created_at", "discount")
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"))

    val firstLastTxn = salesAndWalletTable
      .addFirstLastCol(partitionCol = Seq("customer_id", "dt"))

    val promocodeAggregates = DataTables.allPromoUsagesTable(spark, settings.datalakeDfs.promoCodeUsage, targetDateStr, startDate)
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), isL1 = true)

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    val dataDF = {
      salesAggregates
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(promocodeAggregates, Seq("customer_id", "dt"), "left_outer")
        .renameColumns(prefix = "L1_", excludedColumns = Seq("customer_id", "dt"))
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL1Path)

    // Generate GA Aggregates
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L1/"

    val gaTable = spark.read.parquet(gaAggPath)
    val gaAggregates = gaTable.select(
      "customer_id",
      "dt",
      "product_views_app",
      "product_clicks_app",
      "product_views_web",
      "product_clicks_web",
      "pdp_sessions_app",
      "pdp_sessions_web"
    )
      .addGAAggregates(groupByCol = Seq("customer_id", "dt"))

    val gaAggregateL1 = gaAggregates
      .renameColumns(prefix = "L1GA_", excludedColumns = Seq("customer_id", "dt"))
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL1.moveHDFSData(dtGASeq, aggGABasePath)

  }

}