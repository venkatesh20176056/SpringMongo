package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._

object AggFeaturesL2 extends AggFeaturesL2Job
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL2Job {
  this: SparkJob =>

  val JobName = "AggFeaturesL2"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    //GA specific dates
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val dtGASeq = dayIterator(targetDateGA.minusDays(gaBackFillDays), targetDateGA, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggL2Path = s"${baseDFS.aggPath}/L2"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val anchorPath = s"${baseDFS.anchorPath}/L2"

    import spark.implicits._

    // Make the DataFrame
    val ccMap = DataTables.catalogCategoryTable(spark, settings.datalakeDfs.catalog_category).getCCMap()

    // Unfiltered with Transaction status for last order transaction date feature
    val salesTableUnfiltered = spark.read.parquet(baseDFS.salesPromoCategoryPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .where($"isCatalogProductJoin".isNotNull) //now it has product table data
      .addL2LevelCol(ccMap)
      .where($"L2_Level".isNotNull && ($"L2_Level" =!= ""))
      .repartition($"dt", $"customer_id")

    val salesAggregatesUnfilteredRU =
      salesTableUnfiltered
        .select("customer_id", "dt", "L2_Level", "order_item_id", "selling_price", "created_at", "discount")
        .where($"L2_Level" === "RU")
        .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "L2_Level", unFiltered = true)
        .renameColsWithSuffix("_last_attempt_order_date", excludedColumns = Seq("customer_id", "dt"))

    // Filtered with Transaction status
    val salesTable = salesTableUnfiltered.applySuccessfulFilter

    // Generate Sales Aggregate
    val salesAggregates =
      salesTable
        .select("customer_id", "dt", "L2_Level", "order_item_id", "selling_price", "created_at", "discount")
        .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "L2_Level")

    // Generate Sales First and Last Aggregate
    val firstLastTxn =
      salesTable
        .select("customer_id", "dt", "L2_Level", "selling_price", "created_at")
        .addFirstLastCol(partitionCol = Seq("customer_id", "dt", "L2_Level"))
        .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "L2_Level")

    // Generate Promocode Aggregates
    val promocodeAggregates = salesTable // salestables already has promocode data
      .where($"isPromoCodeJoin".isNotNull) //to implement inner join check whether promoJoin is not null
      .select("customer_id", "dt", "order_item_id", "amount", "L2_Level") // select relevant columns for groupby
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "L2_Level")

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    val dataDF = {
      salesAggregatesUnfilteredRU
        .join(salesAggregates, Seq("customer_id", "dt"), "full_outer")
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(promocodeAggregates, Seq("customer_id", "dt"), "left_outer")
        .renameColumns(prefix = "L2_", excludedColumns = Seq("customer_id", "dt"))
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL2Path)

    // Generate GA Aggregates
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L2/"

    val gaTable = spark.read.parquet(gaAggPath).addL2LevelColGA(ccMap)
    val gaAggregates = gaTable.select(
      "customer_id",
      "dt",
      "L2_Level",
      "product_views_app",
      "product_clicks_app",
      "product_views_web",
      "product_clicks_web",
      "pdp_sessions_app",
      "pdp_sessions_web"
    )
      .addGAAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "L2_Level")

    val gaAggregateL2 = gaAggregates
      .renameColumns(prefix = "L2GA_", excludedColumns = Seq("customer_id", "dt"))
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL2.moveHDFSData(dtGASeq, aggGABasePath)

  }

  implicit class L2Implicits(dF: DataFrame) {
    import dF.sparkSession.implicits._

    /**
     * Notes:
     * Fastag (vertical_id = 69) is under EC business vertical. It should be part of RU.
     */
    val bookingLogic: Column = $"vertical_id".isin(64, 70, 74, 66, 85, 72, 60, 104, 40, 73, 29, 52, 81, 26)
    val rechargeLogic: Column = $"business_vertical_id".isin(1, 5) || $"vertical_id" === 69
    val ecommerceLogic: Column = $"business_vertical_id".isin(4) && $"vertical_id" =!= 69

    def getCCMap(isCategoryKey: Boolean = true): Map[Int, Int] = {
      val keyName = if (isCategoryKey) "category_id" else "vertical_id"
      dF.select(keyName, "business_vertical_id")
        .distinct // To remove duplicates entry because of the category_id,business_vertical_id combo
        .collect()
        .map(row => (row.getAs[Int](0), row.getAs[Int](1)))
        .toMap
    }

    def addL2LevelCol(ccMap: Map[Int, Int], isCategoryKey: Boolean = true): DataFrame = {
      val addBusinessVertical: UserDefinedFunction = udf((verticalId: Int) => ccMap.getOrElse(verticalId, 0))
      val keyName = if (isCategoryKey) "category_id" else "vertical_id"

      dF
        .withColumn("business_vertical_id", addBusinessVertical(col(keyName)))
        .withColumn(
          "L2_Level", when(bookingLogic, lit("BK"))
            .when(rechargeLogic, lit("RU"))
            .when(ecommerceLogic, lit("EC")).otherwise(lit(null))
        )
    }

    def applySuccessfulFilter: DataFrame = {
      import dF.sparkSession.implicits._

      dF
        .withColumn(
          "is_successful_txn",
          when(($"L2_Level" === "EC").and($"successfulTxnFlagEcommerce" === 1), lit(1))
            .when($"successfulTxnFlag" === 1, lit(1))
            .otherwise(lit(null))
        ).where($"is_successful_txn" === 1)
    }

    def addL2LevelColGA(ccMap: Map[Int, Int]): DataFrame = {
      import dF.sparkSession.implicits._
      val addBusinessVertical: UserDefinedFunction = udf((verticalId: Int) => ccMap.getOrElse(verticalId, 0))

      dF
        .withColumn("business_vertical_id", addBusinessVertical($"category_id"))
        .withColumn(
          "L2_Level",
          when(bookingLogic, lit("BK")).when(ecommerceLogic, lit("EC")).otherwise(lit(null))
        ).where($"L2_Level".isNotNull && ($"L2_Level" =!= ""))
    }
  }

}