package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.ECSchema.L3EC2Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGAEC2.GAL3EC2Schema
import com.paytm.map.features.config.Schemas.SchemaRepoGA.GAL3EC2LPSchema
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._

object AggFeaturesL3EC2 extends AggFeaturesL3EC2Job
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL3EC2Job {
  this: SparkJob =>

  val JobName = "AggFeaturesL3EC2"

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
    val aggL3EC2Path = s"${baseDFS.aggPath}/L3EC2"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)
    val gaLPAggPath = settings.featuresDfs.baseDFS.gaAggregateLPPath(targetDateGA)
    val anchorPath = s"${baseDFS.anchorPath}/L3EC2"

    import spark.implicits._

    //provided by rahul prassad
    //TODO: Flag these categories in cat map
    val requiredCategoryList = Seq(5029, 5041, 5048, 7575, 8981, 8986, 8996, 41129, 49090, 64161, 5180, 5187, 5246, 5254, 5267,
      5240, 7467, 5276, 7515, 7534, 41104, 49120, 64168, 5189, 5191, 5288, 5293, 5299, 5322, 5335, 6240, 6371, 78462,
      6452, 6498, 7703, 8027, 8589, 17211, 78457, 24717, 105809, 5404, 5433, 5435, 5437, 5443, 5582, 5465, 5468, 5473,
      5479, 5482, 5484, 5486, 5489, 5491, 5493, 5502, 5507, 5509, 5511, 5513, 5515, 5517, 5943, 7648, 6296, 6297, 6298,
      6299, 6300, 6301, 6302, 6303, 6304, 6305, 8148, 16860, 20581, 26003, 26120, 26122, 26124, 26125, 26126, 47689,
      51396, 51552, 51599, 88905, 104611, 6443, 17422, 6224, 7908, 67591, 6560, 6576, 6592, 6602, 6635, 6650, 6679,
      6689, 6697, 12176, 12787, 12793, 15640, 18882, 23038, 38632, 38775, 58518, 6613, 8215, 8216, 8217, 79042, 79048,
      10022, 12739, 12742, 12743, 12741, 12738, 23095, 12744, 23192, 23193, 10108, 10109, 10110, 10111, 10112, 10114,
      41667, 41676, 41829, 41827, 66677, 10524, 10525, 10526, 10527, 10528, 10529, 14951, 14952, 40038, 40039, 40040,
      40042, 40043, 40044, 40046, 40048, 40049, 40051, 40047, 79053, 79057, 80189, 18714, 24283, 26422, 21341, 21342,
      21343, 21344, 21345, 21346, 21347, 21348, 21349, 21350, 21351, 21353, 21354, 21360, 21372, 21356, 21385, 21357,
      21391, 21393, 21388, 21563, 21564, 21565, 21567, 21569, 21618, 21633, 21649, 21655, 21662, 21666, 21672, 21566,
      63434, 74043, 84176, 21987, 21988, 21990, 21991, 21994, 22029, 22032, 22034, 22035, 23096, 66828, 77455, 31561,
      31562, 31563, 31564, 31567, 31569, 31566, 39036, 39038, 39042, 39043, 39035, 39037, 39039, 39040, 39041, 49291,
      59462, 65062, 66786, 69443, 69448, 72801, 111269, 74600, 74601, 84768, 84778, 88719, 93845, 114018, 101311, 101403,
      101405, 101401, 101402, 101404, 114316, 101332, 103154, 103156, 103158, 103161, 103162, 118711, 167784)

    val categoryMap = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(catMapPath)
      .select(
        $"id".cast(IntegerType).as("category_id"),
        $"L2".cast(IntegerType).as("L2")
      ).where($"L2".isNotNull && ($"L2" > 0))
      .where($"category_id".isNotNull && ($"category_id" > 0))
      .collect()
      .map(row => (row.getAs[Int](0), row.getAs[Int](1)))
      .filter(x => requiredCategoryList.contains(x._2))
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
        .renameColumns(prefix = "L3_EC2_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(L3EC2Schema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL3EC2Path)

    //GA aggregates
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L3EC2/"

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
      .renameColumns(prefix = "L3GA_EC2_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC2Schema)

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
      .renameColumns(prefix = "L3GA_EC2_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3EC2LPSchema)

    // Joining Product and Landing PAge Aggregates
    val gaAggregateL3 = gaProductAggregateL3.join(gaLPAggregateL3, Seq("customer_id", "dt"), "left_outer")
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL3.moveHDFSData(dtGASeq, aggGABasePath)
  }

  private implicit class L3EC2Implicits(dF: DataFrame) {

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