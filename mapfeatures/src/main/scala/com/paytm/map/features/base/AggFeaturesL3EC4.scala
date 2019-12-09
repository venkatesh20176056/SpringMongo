package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC4.L3EC4Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC4.GAL3EC4Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC4.GAL3EC4LPSchema
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._

object AggFeaturesL3EC4 extends AggFeaturesL3EC4Job
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL3EC4Job {
  this: SparkJob =>

  val JobName = "AggFeaturesL3EC4"

  //108 categories, provided by business as listed on https://jira.mypaytm.com/browse/MAP-450
  val requiredCategoryListL4 = Seq(125120, 60009, 6309, 6453, 11420, 6241, 23633, 6244, 6373, 6492,
    28996, 6242, 5030, 8543, 8544, 5035, 5241, 5242, 5053, 5049, 5050, 5413, 77978, 5057, 8997, 66781,
    6499, 24838, 24843, 29161, 24837, 24194, 6502, 27601, 17628, 24836, 7710, 6501, 30193, 30161,
    101450, 101440, 101455, 101384, 101387, 101390, 101385, 101391, 101449, 101431, 101425, 101504,
    101381, 101395, 101325, 101322, 101326, 101551, 101544, 101545, 101467, 101471, 101472, 101396,
    101375, 101315, 101460, 101452, 101554, 101420, 101333, 101553, 101314, 101509, 101419, 27220,
    11641, 25126, 6372, 6375, 6374, 18231, 83048, 32596, 8591, 8028, 8029, 8030, 9182, 15594, 15595,
    24839, 27221, 30041, 30164, 30165, 30725, 48721, 81148, 27620, 67574, 5232, 7445, 8731, 6036,
    66780, 66782, 66783, 26400, 15627, 5238, 5932, 5846, 6017, 8139,
    15343, 101556, 101323, 40343, 26174, 14887, 8732, 5684, 41830,
    5037, 19686, 101444, 101378, 6451, 5233, 89695,
    73307, 6899, 8142, 25986, 8137, 6316, 8548, 73308, 6364, 6363, 52798, 52792, 6355, 6361,
    26579, 101511, 101456, 101507, 73777, 73775, 8145, 8146, 6449, 8593, 10171, 18986, 20631, 26744, 6541)

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
    val aggL3EC4Path = s"${baseDFS.aggPath}/L3EC4"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val gaLPAggPath = settings.featuresDfs.baseDFS.gaAggregateLPPath(targetDateGA)
    val anchorPath = s"${baseDFS.anchorPath}/L3EC4"

    import spark.implicits._

    val categoryMap = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(catMapPath)
      .select(
        $"id".cast(IntegerType).as("category_id"),
        $"L4".cast(IntegerType).as("L4")
      ).where($"L4".isNotNull && ($"L4" > 0))
      .where($"category_id".isNotNull && ($"category_id" > 0))
      .collect()
      .map(row => (row.getAs[Int](0), row.getAs[Int](1)))
      .filter(x => requiredCategoryListL4.contains(x._2))
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
        .renameColumns(prefix = "L3_EC4_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(L3EC4Schema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL3EC4Path)

    //GA aggregates
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L3EC4/"

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
      .renameColumns(prefix = "L3GA_EC4_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC4Schema)

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
      .renameColumns(prefix = "L3GA_EC4_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC4LPSchema)

    // Joining Product and Landing PAge Aggregates
    val gaAggregateL3 = gaProductAggregateL3.join(gaLPAggregateL3, Seq("customer_id", "dt"), "left_outer")
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL3.moveHDFSData(dtGASeq, aggGABasePath)
  }

  private implicit class L3EC4Implicits(dF: DataFrame) {

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