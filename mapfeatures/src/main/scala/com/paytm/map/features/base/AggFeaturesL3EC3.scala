package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.ECSchema.L3EC3Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGA.GAL3EC3Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGA.GAL3EC3LPSchema
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._

object AggFeaturesL3EC3 extends AggFeaturesL3EC3Job
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL3EC3Job {
  this: SparkJob =>

  val JobName = "AggFeaturesL3EC3"

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
    val catMapPath = settings.featuresDfs.resources + "category_mapping"
    val baseDFS = settings.featuresDfs.baseDFS
    val aggL3EC3Path = s"${baseDFS.aggPath}/L3EC3"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val gaLPAggPath = settings.featuresDfs.baseDFS.gaAggregateLPPath(targetDateGA)
    val anchorPath = s"${baseDFS.anchorPath}/L3EC3"

    import spark.implicits._

    //6 categories provided by business as listed on https://jira.mypaytm.com/browse/MAP-450
    val requiredCategoryListL3 = Seq(5928, 5038, 78658, 101416, 101332, 101533, 14886, 78654, 101532, 101383,
      52771, 73773, 51613, 6359, 26378, 26484, 101353, 101502, 101474, 114318, 26483, 8143, 78344, 78345, 78346)

    val categoryMap = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(catMapPath)
      .select(
        $"id".cast(IntegerType).as("category_id"),
        $"L3".cast(IntegerType).as("L3")
      ).where($"L3".isNotNull && ($"L3" > 0))
      .where($"category_id".isNotNull && ($"category_id" > 0))
      .collect()
      .map(row => (row.getAs[Int](0), row.getAs[Int](1)))
      .filter(x => requiredCategoryListL3.contains(x._2))
      .toMap

    val salesPromoTable = spark.read.parquet(baseDFS.salesPromoCategoryPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .where(col("successfulTxnFlagEcommerce") === 1)
      .where($"isCatalogProductJoin".isNotNull) // to implement inner join between SO,SOI,Promo
      .where($"category_id".isNotNull)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .addECLevel(categoryMap)
      .repartition($"dt", $"customer_id")
      .cache

    // Generate Sales Aggregate
    val salesAggregates = salesPromoTable
      .select("customer_id", "dt", "EC_Level", "order_item_id", "selling_price", "created_at", "discount")
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "EC_Level", isDiscount = false)

    // Generate Sales First and Last Aggregate
    val firstLastTxn = salesPromoTable
      .select("customer_id", "dt", "EC_Level", "selling_price", "created_at")
      .addFirstLastCol(Seq("customer_id", "dt", "EC_Level"))
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "EC_Level")

    // Preferred Brand Count,Size
    val preferableBrandData = salesPromoTable.addPreferredBrandFeat("EC_Level")

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    val dataDF = {
      salesAggregates
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(preferableBrandData, Seq("customer_id", "dt"), "left_outer")
        .renameColumns(prefix = "L3_EC3_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(L3EC3Schema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL3EC3Path)

    //GA aggregates
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L3EC3/"

    val gaTable = spark.read.parquet(gaAggPath).addECLevel(categoryMap)

    val gaAggregates = gaTable.select(
      "customer_id",
      "dt",
      "EC_Level",
      "product_views_app",
      "product_clicks_app",
      "product_views_web",
      "product_clicks_web",
      "pdp_sessions_app",
      "pdp_sessions_web"
    )
      .addGAAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "EC_Level")

    val gaProductAggregateL3 = gaAggregates
      .renameColumns(prefix = "L3GA_EC3_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC3Schema)

    //GA Landing Page aggregates
    val gaLPTable = spark.read.parquet(gaLPAggPath).addECLevel(categoryMap)

    val gaLPAggregates = gaLPTable.select(
      "customer_id",
      "dt",
      "EC_Level",
      "clp_sessions_app",
      "clp_sessions_web",
      "glp_sessions_app",
      "glp_sessions_web"
    )
      .addGALPAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "EC_Level")

    val gaLPAggregateL3 = gaLPAggregates
      .renameColumns(prefix = "L3GA_EC3_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC3LPSchema)

    // Joining Product and Landing PAge Aggregates
    val gaAggregateL3 = gaProductAggregateL3.join(gaLPAggregateL3, Seq("customer_id", "dt"), "left_outer")
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL3.moveHDFSData(dtGASeq, aggGABasePath)
  }

  private implicit class L3EC3Implicits(dF: DataFrame) {

    def addECLevel(categoryMap: Map[Int, Int]): DataFrame = {

      import dF.sparkSession.implicits._

      val getT2 = udf((x: Int) => categoryMap.getOrElse(x, -1))

      dF.withColumn("EC_Category", getT2($"category_id"))
        .where($"EC_Category".isNotNull)
        .where($"EC_Category" > -1)
        .withColumn("EC_Level", concat(lit("CAT"), $"EC_Category"))
        .where($"EC_Level".isNotNull)
        .drop("EC_Category")
    }
  }

}