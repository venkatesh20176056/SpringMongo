package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.UDFs.readTableV3
import com.paytm.map.features.utils.{ArgsUtils, CMACatMapping, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object SignalFeatures extends SignalFeaturesJob
  with SparkJob with SparkJobBootstrap

trait SignalFeaturesJob {
  this: SparkJob =>

  val JobName = "SignalFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark
    import spark.implicits._

    // Get the command line parameters
    val targetDate = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    val catalogCategory = readTableV3(spark, settings.datalakeDfs.catalog_category)
      .select($"id" as "category_id", $"name" as "category_name", $"url_key" as "category_url_key")

    val catalogProduct = readTableV3(spark, settings.datalakeDfs.catalog_product)
      .select($"id" as "product_id", $"name" as "product_name", $"brand" as "brand", $"category_id")
      .join(catalogCategory, Seq("category_id"))
      .cache

    val catMapping = CMACatMapping.getT4Mappings(spark)

    val catNameMapping = CMACatMapping.getCategoryNameMappings(spark)

    val signalPath = s"${settings.featuresDfs.baseDFS.deviceSignalPath}/dt=$targetDateStr"
    val signalDF = spark
      .read
      .parquet(signalPath)
      .withColumn("dt", to_date(correctEventTime))
      .withColumnRenamed("customerId", "customer_id")
      .repartition($"dt", $"customer_id")

    // Parse Path Config
    val aggPartitionedPath = s"${settings.featuresDfs.baseDFS.aggPath}/SignalFeatures"

    val browsingFeatureDF = browsingFeatures(signalDF, targetDateStr, catalogProduct, catMapping, catNameMapping)

    val scanScreenDF = payScreenFlag(signalDF)

    val paytmFirstDF = paytmFirstFeatures(signalDF)

    searchFeatures(signalDF, targetDateStr, catMapping, catNameMapping)
      .join(browsingFeatureDF, Seq("customer_id", "dt"), "full_outer")
      .join(scanScreenDF, Seq("customer_id", "dt"), "full_outer")
      .join(paytmFirstDF, Seq("customer_id", "dt"), "full_outer")
      .renameColumns("SIGNAL_", Seq("customer_id", "dt"))
      .coalescePartitions("dt", "customer_id", dtSeq)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .parquet(s"$aggPartitionedPath/targetDate=$targetDateStr")
    //Since corrected event time can fall between multiple days, partition by both targetDate and dt

  }

  def payScreenFlag(signalDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val payScreenSchema =
      StructType(Array(
        StructField("event_action", StringType),
        StructField("screenName", StringType)
      ))

    val filter: Column = ($"parsed_payload.screenName" === "/wallet/pay-send")

    signalDF
      .withColumn("parsed_payload", from_json($"payload", payScreenSchema))
      .withColumn("pay_screen_flag", when(filter, 1).otherwise(null))
      .groupBy("customer_id", "dt")
      .agg(max("pay_screen_flag").alias("pay_screen_flag"))
  }

  def searchFeatures(signalDF: DataFrame, targetDate: String, categoryMapping: DataFrame, categoryNameMapping: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val payloadSchema =
      StructType(Array(
        StructField("search_within_category", StringType),
        StructField("search_ab_value", StringType),
        StructField("search_category", StringType),
        StructField("search_query", StringType),
        StructField("search_result_type", StringType),
        StructField("search_term", StringType),
        StructField("brandstore_search_term", StringType),
        StructField("movie_search_category", StringType)
      ))

    val brandList = Seq("apple", "samsung", "vivo", "oppo", "redmi", "nokia", "honor", "google", "motorola", "lenovo", "asus", "hp", "dell", "acer", "microsoft", "msi", "iball", "epson")

    val getBrands = udf { query: String =>
      val queryLower = if (query != null) query.toLowerCase else ""

      val brands = brandList.flatMap { brand =>
        if (queryLower.contains(brand)) List(brand) else List()
      }

      if (brands.length == 0) None else Some(brands)
    }

    signalDF //.withColumn("dt", lit(targetDate))
      .filter($"customer_id".isNotNull and $"customer_id" =!= "")
      .withColumn("parsed_payload", from_json($"payload", payloadSchema))
      .select(
        $"customer_id",
        $"dt",
        $"parsed_payload.search_within_category",
        $"parsed_payload.search_category",
        coalesce($"parsed_payload.search_query", $"parsed_payload.search_term", $"parsed_payload.brandstore_search_term") as "search_query"
      )
      .filter($"search_within_category".isNotNull || $"search_category".isNotNull || $"search_query".isNotNull)
      .withColumn("brand_searched", explode(getBrands($"search_query")))
      .join(categoryMapping, $"category_id" === $"search_category")
      .join(categoryNameMapping, categoryMapping("map_category_id") === categoryNameMapping("id")).select(
        $"customer_id",
        $"dt",
        $"brand_searched",
        $"name".alias("category_searched")
      )
      .groupBy("customer_id", "dt")
      .agg(
        collect_set($"brand_searched").alias("brands_searched"),
        collect_set($"category_searched").alias("categories_searched")
      )

  }

  def browsingFeatures(signalDF: DataFrame, targetDate: String, catalogProduct: DataFrame, categoryMapping: DataFrame, categoryNameMapping: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val browsingSchema =
      StructType(Seq(
        StructField("event", StringType),
        StructField(
          "ecommerce",
          StructType(Seq(
            StructField(
              "impressions",
              ArrayType(
                StructType(Seq(
                  StructField("id", StringType),
                  StructField("name", StringType)
                ))
              )
            ),
            StructField(
              "click",
              StructType(Seq(
                StructField(
                  "products",
                  ArrayType(
                    StructType(Seq(
                      StructField("id", StringType),
                      StructField("name", StringType)
                    ))
                  )
                )
              ))
            )
          ))
        )
      ))

    val filteredSignalDF = signalDF
      .withColumn("parsed_payload", from_json($"payload", browsingSchema))

    val impressions = filteredSignalDF
      .filter($"parsed_payload.event" === "productImpression")
      .withColumn("impressions", explode($"parsed_payload.ecommerce.impressions"))
      .select(
        $"customer_id",
        $"dt",
        $"impressions.id".alias("product_id"),
        $"impressions.name".alias("product_name")
      )
      .join(catalogProduct, Seq("product_id"))
      .join(broadcast(categoryMapping), Seq("category_id"), "left_outer")
      .join(broadcast(categoryNameMapping), categoryMapping("map_category_id") === categoryNameMapping("id"), "left_outer")
      .groupBy("customer_id", "dt")
      .agg(collect_set($"name").alias("categories_browsed"))

    val clicks = filteredSignalDF
      .filter($"parsed_payload.event" === "productClick")
      .withColumn("clicks", explode($"parsed_payload.ecommerce.click.products"))
      .select(
        $"customer_id",
        $"dt",
        $"clicks.id".alias("product_id"),
        $"clicks.name".alias("product_name")
      )
      .join(catalogProduct, Seq("product_id"))
      .join(broadcast(categoryMapping), Seq("category_id"), "left_outer")
      .join(broadcast(categoryNameMapping), categoryMapping("map_category_id") === categoryNameMapping("id"), "left_outer")
      .groupBy("customer_id", "dt")
      .agg(collect_set($"name").alias("categories_clicked"))

    impressions.join(clicks, Seq("customer_id", "dt"), "left_outer")
  }

  def paytmFirstFeatures(signalDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val paytmFirstSchema = StructType(Array(
      StructField("screenName", StringType)
    ))

    val paytmFirstScreen = "/prime/join"

    signalDF.withColumn("parsed_payload", from_json($"payload", paytmFirstSchema))
      .filter($"parsed_payload.screenName" === paytmFirstScreen)
      .withColumn("dt", to_date(correctEventTime))
      .groupBy("customer_id", "dt")
      .agg(lit(1).alias("paytm_first_screen_visited"))
  }

  def correctEventTime()(implicit spark: SparkSession): Column = {
    def toTime(column: Column) = from_utc_timestamp(from_unixtime(column / 1000), "Asia/Kolkata")

    import spark.implicits._
    toTime(when(
      $"uploadTime" >= $"deviceDateTime",
      least($"dateTime", $"dateTime" - $"uploadTime" + $"deviceDateTime")
    )
      .otherwise(least($"dateTime", $"deviceDateTime")))
  }
}
