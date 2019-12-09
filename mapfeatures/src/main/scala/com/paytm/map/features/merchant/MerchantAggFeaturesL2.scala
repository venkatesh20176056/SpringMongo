package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.ConvenientFrame.configureSparkForMumbaiS3Access
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._
import com.paytm.map.features.base.DataTables

object MerchantAggFeaturesL2 extends MerchantAggFeaturesL2Job
  with SparkJob with SparkJobBootstrap

trait MerchantAggFeaturesL2Job {
  this: SparkJob =>

  val JobName = "MerchantAggFeaturesL2"

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
    val aggL2Path = s"${baseDFS.aggPath}/MerchantL2"
    val anchorPath = s"${baseDFS.anchorPath}/MerchantL2"

    import spark.implicits._

    // Make the DataFrame
    val ccMap = DataTables.catalogCategoryTable(spark, settings.datalakeDfs.catalog_category).getCCMap()

    // Unfiltered with Transaction status for last order transaction date feature
    val salesTable = spark.read.parquet(baseDFS.salesPromoCategoryPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .withColumnRenamed("dmid", "merchant_id")
      .where($"isCatalogProductJoin".isNotNull) //now it has product table data
      .addL2LevelCol(ccMap)
      .where($"L2_Level".isNotNull && ($"L2_Level" =!= ""))
      .applySuccessfulFilter

    // Generate Sales Aggregate
    val aggregates = Seq(
      count(col("order_item_id")).as("total_transaction_count"),
      sum(col("selling_price")).as("total_transaction_size")
    )
    val salesAggregates = salesTable
      .groupPivotAgg(Seq("merchant_id", "dt"), pivotCol = "L2_Level", aggregates, pivotValues = Seq("BK", "EC", "RU"))

    // Final Aggregates
    val dataDF = {
      salesAggregates
        .renameColumns(prefix = "L2_", excludedColumns = Seq("merchant_id", "dt"))
        .repartition(20, $"dt")
        .write
        .partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL2Path)
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
  }

}