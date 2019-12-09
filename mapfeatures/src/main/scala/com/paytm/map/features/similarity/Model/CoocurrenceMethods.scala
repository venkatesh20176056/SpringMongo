package com.paytm.map.features.similarity.Model

import com.paytm.map.features.utils.UDFs.readTable
import com.paytm.map.features.utils.{FileUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

trait CoocurrenceMethods extends SimilarUserMethods {
  def loadPurchaseData(spark: SparkSession, settings: Settings, catalog_product: DataFrame,
    targetDateStr: String, startDateStr: String): DataFrame = {
    import spark.implicits._

    val soi = readTable(spark, settings.datalakeDfs.salesOrderItem, "marketplace", "sales_order_item", targetDateStr, startDateStr, isV2 = false)
      .select($"created_at".cast(StringType), $"order_id".cast(LongType) as "order_id", $"status", $"product_id", $"price".cast(LongType) as "price", $"vertical_id")

    val so = readTable(spark, settings.datalakeDfs.salesOrder, "marketplace", "sales_order", targetDateStr, startDateStr, isV2 = false)
      .select($"id".cast(LongType) as "order_id", $"customer_id".cast(LongType), $"payment_status")

    // Work
    val sales = so.join(soi, Seq("order_id")).filter($"payment_status" === 2)
      .select(col("customer_id"), $"product_id", split($"created_at", " ").getItem(0) as "date")
      .join(catalog_product, Seq("product_id")).drop($"product_id").drop("product_name")
      .repartition($"customer_id")

    sales
  }

  def createCoocurrence(
    spark: SparkSession,
    sales: DataFrame,
    workingDir: String,
    jobSuffix: String,
    num_worker: Int
  ): String = {
    import spark.implicits._
    val cooccurencePath = s"${workingDir}/cooccurence_count_${jobSuffix}"
    if (!FileUtils.pathExists(cooccurencePath + "/_SUCCESS", spark)) {
      val validCustomersPath = s"${workingDir}/valid_customers_${jobSuffix}"
      val selfJoinedSalesPath = s"${workingDir}/self_joined_sales_${jobSuffix}"
      val coocurrenceByCustomersPath = s"${workingDir}/coocurrence_by_customers_${jobSuffix}"

      sales.groupBy("customer_id")
        .agg(count($"customer_id") as "customer_count")
        .filter($"customer_count" > 1 && $"customer_count" < 100)
        .repartition($"customer_id")
        .write.mode("overwrite").parquet(validCustomersPath)

      val valid_customers = spark.read.parquet(validCustomersPath)
      sales.join(valid_customers, Seq("customer_id"))
        .join(
          sales.select($"customer_id" as "r_customer_id", $"category_id" as "r_category_id"),
          $"customer_id" === $"r_customer_id" and $"category_id" < $"r_category_id"
        )
        .repartition($"customer_id")
        .write.mode("overwrite").parquet(selfJoinedSalesPath)
      valid_customers.unpersist()

      val selfJoinedSales = spark.read.parquet(selfJoinedSalesPath)
      selfJoinedSales.groupBy("category_id", "r_category_id", "date", "customer_id")
        .agg(count($"customer_id") as "occurence_count", lit(1) as "customer_count")
        .drop("date")
        .repartition($"customer_id")
        .write.mode("overwrite").parquet(coocurrenceByCustomersPath)
      selfJoinedSales.unpersist()

      val cooccurrenceByCustomers = spark.read.parquet(coocurrenceByCustomersPath)
      cooccurrenceByCustomers.groupBy($"category_id", $"r_category_id")
        .agg(sum("occurence_count") as "occurence_count", sum("customer_count") as "customer_count")
        .select($"category_id" as "ida", $"r_category_id" as "idb", $"occurence_count", $"customer_count")
        .rdd.flatMap {
          case Row(ida: Long, idb: Long, occurenceCount: Long, customerCount: Long) =>
            Array(
              CooccurenceCount(ida, idb, occurenceCount, customerCount),
              CooccurenceCount(idb, ida, occurenceCount, customerCount)
            )
        }.toDF.write.mode("overwrite").parquet(cooccurencePath)
      cooccurrenceByCustomers.unpersist()
    }
    cooccurencePath
  }

  def computeLookaLikeUser(
    spark: SparkSession,
    sales_df: DataFrame,
    cooDFAgg: DataFrame,
    targetDateStr: String,
    userBase: DataFrame,
    maximumUsers: Int,
    lookALikeUserPath: String
  ) {
    import spark.implicits._
    if (!FileUtils.pathExists(lookALikeUserPath + "/_SUCCESS", spark)) {
      val baseUserItems = sales_df.select("customer_id", "category_id")
        .join(userBase, Seq("customer_id"))
        .distinct

      val otherUserItems = sales_df.select("customer_id", "category_id")
        .join(userBase, Seq("customer_id"), "left_anti")

      val baseCategories = baseUserItems
        .join(broadcast(cooDFAgg), Seq("category_id"))
        .select($"customer_id" as "base_customer", explode($"coocurrence") as "category_id")
        .select("category_id").distinct

      val lookAlikeUsers = otherUserItems.join(broadcast(baseCategories), "category_id")
        .groupBy($"customer_id").count
        .orderBy(desc("count"))
        .limit(maximumUsers)

      lookAlikeUsers
        .select($"customer_id" as "CustomerId")
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(lookALikeUserPath)
    }
  }

  def ComputeAndWriteSimilarUsers(spark: SparkSession, settings: Settings,
    userBaseDF: DataFrame, similarUsersOutputPath: String,
    sharedOutputPath: String, sharedOutputSuffix: String,
    executionOutputPath: String,
    targetDateStr: String, startDateStr: String, maxNumUsers: Int, numWorkers: Int = 40) {
    import spark.implicits._

    val catalog_product = readTable(spark, settings.datalakeDfs.catalog_product,
      "marketplace_catalog", "catalog_product", targetDateStr, isV2 = false)
      .select($"id" as "product_id", $"category_id", $"name" as "product_name")
    val sales = loadPurchaseData(spark, settings, catalog_product, targetDateStr, startDateStr)

    val cooccurencePath = createCoocurrence(spark, sales, sharedOutputPath, sharedOutputPath, numWorkers)

    val cooDF = spark.read.parquet(cooccurencePath)

    val cooDFAgg = cooDF
      .orderBy(desc("occurence_count"))
      .groupBy($"ida").agg(collect_list("idb") as "coocurrence")
      .withColumnRenamed("ida", "category_id")

    computeLookaLikeUser(spark, sales, cooDFAgg, targetDateStr,
      userBaseDF, maxNumUsers, similarUsersOutputPath)
  }
}
