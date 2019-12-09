package com.paytm.map.features.similarity.Model

import com.paytm.map.features.utils.UDFs.readTable
import com.paytm.map.features.utils.{FileUtils, Settings}
import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
import com.paytm.map.features.utils.{FileUtils, Settings}
import org.apache.spark.sql.functions.{broadcast, lower, udf}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

trait HashMethods {
  def loadPurchaseData(spark: SparkSession, settings: Settings, targetDateStr: String, startDateStr: String, rechargeFiltered: Boolean = true): DataFrame = {
    import spark.implicits._
    val soi = readTable(spark, settings.datalakeDfs.salesOrderItem, "marketplace", "sales_order_item_snapshot_v2", targetDateStr, startDateStr, isV2 = false)
      .select($"id".cast(LongType) as "item_id", $"created_at".cast(StringType), $"order_id".cast(LongType) as "order_id", $"status", $"product_id", $"price".cast(LongType) as "price")

    val so = readTable(spark, settings.datalakeDfs.salesOrder, "marketplace", "sales_order_snapshot_v2", targetDateStr, startDateStr, isV2 = false)
      .select($"id".cast(LongType) as "order_id", $"customer_id".cast(LongType), $"payment_status")

    // Work
    val sales = if (rechargeFiltered) {
      val catalogVerticalRecharge = readTable(
        spark,
        settings.datalakeDfs.catalogVerticalRecharge,
        "fs_recharge", "catalog_vertical_recharge",
        minDate = "full", maxDate = "full", isV2 = false
      )
        .filter(lower($"service").like("%mobile%") && lower($"paytype").like("%prepaid%"))
      so.join(soi, Seq("order_id"))
        .join(catalogVerticalRecharge, Seq("product_id"), "left_anti")
    } else {
      so.join(soi, Seq("order_id"))
    }
    sales
  }

  def ComputeAndWriteSimilarUsers(spark: SparkSession, settings: Settings,
    userBaseDF: DataFrame, similarUsersOutputPath: String,
    sharedOutputPath: String, sharedOutputSuffix: String,
    executionOutputPath: String,
    targetDateStr: String, startDateStr: String, maxNumUsers: Int) {
    val purchaseHashesPath = s"$sharedOutputPath/similartiy_hashes_${sharedOutputSuffix}"
    val purchaseHashesDistinctPath = s"$sharedOutputPath/similartiy_hashes_distinct_${sharedOutputSuffix}"
    val purchaseSequenceLength = 100
    val similarityWindow = 3
    val scoreThreshold = 0.6

    val sales = loadPurchaseData(spark, settings, targetDateStr, startDateStr, rechargeFiltered = true)
    val baseSimilarPurchasesPath = s"${executionOutputPath}/customers_purchases"
    val baseSimilarUsersPath = s"${executionOutputPath}/similar_users"
    computePurchaseHashes(spark, sales, purchaseSequenceLength,
      similarityWindow, purchaseHashesPath, purchaseHashesDistinctPath)
    computeBaseSimilarUsers(spark, userBaseDF, purchaseHashesDistinctPath, baseSimilarPurchasesPath, baseSimilarUsersPath)
    computeLookalikeUsers(spark, baseSimilarUsersPath, userBaseDF, maxNumUsers,
      scoreThreshold, similarUsersOutputPath)
  }

  def computePurchaseHashes(
    spark: SparkSession,
    sales: DataFrame,
    purchaseSequenceLength: Int,
    similarityWindow: Int,
    purchaseHashesPath: String,
    purchaseHashesDistinctPath: String
  ) {
    import spark.implicits._
    // Compute Similarity hashes

    if (!FileUtils.isFileSuccessfullyCreated(spark, purchaseHashesPath)) {
      sales
        .filter($"payment_status" === 2)
        .select("customer_id", "created_at", "product_id").rdd.map {
          case Row(customerId: Long, createdAt: String, itemId: Long) =>
            (customerId, UserPurchase(createdAt, itemId))
        }.topByKey(purchaseSequenceLength)(Ordering.by(_.createdAt))
        .filter(_._2.length >= similarityWindow).flatMap {
          case (customerId: Long, purchases: Array[UserPurchase]) =>
            val numPurchases = purchases.length
            (0 to numPurchases - similarityWindow).map { i =>
              (purchases.slice(i, i + similarityWindow).map(_.productId.toString).sorted.mkString(","), customerId)
            }
        }.toDF("purchases", "customer_id")
        .write.mode("overwrite").parquet(purchaseHashesPath)
    }

    if (!FileUtils.isFileSuccessfullyCreated(spark, purchaseHashesDistinctPath)) {
      // Identify the distinct similarity hashes
      spark.read.parquet(purchaseHashesPath).distinct.repartition($"purchases")
        .write.mode("overwrite").parquet(purchaseHashesDistinctPath)
    }
  }

  def computeBaseSimilarUsers(
    spark: SparkSession,
    userBase: DataFrame,
    purchaseHashesDistinctPath: String,
    baseSimilarPurchasesPath: String,
    baseSimilarUsersPath: String
  ): String = {
    import spark.implicits._
    // Identify similarity hash from base
    val similarUsersDistinct = spark.read.parquet(purchaseHashesDistinctPath)
    similarUsersDistinct.join(userBase, Seq("customer_id"))
      .select("purchases", "customer_id").distinct
      .repartition($"purchases")
      .write.mode("overwrite").parquet(baseSimilarPurchasesPath)

    // Compute base similar users
    val baseSimilarPurchases = spark.read.parquet(baseSimilarPurchasesPath)
    baseSimilarPurchases.select("purchases").distinct
      .join(similarUsersDistinct, Seq("purchases")).select("customer_id")
      .write.mode("overwrite").parquet(baseSimilarUsersPath)
    baseSimilarUsersPath
  }

  def computeLookalikeUsers(
    spark: SparkSession,
    baseSimilarUsersPath: String,
    userBase: DataFrame,
    userLimit: Int,
    scoreThreshold: Double,
    baseSimilarUsersScoredPath: String
  ) {
    import spark.implicits._
    val baseSimilarUsers = spark.read.parquet(baseSimilarUsersPath)
    if (baseSimilarUsers.count > 0) {
      val baseSimilarStats = baseSimilarUsers.groupBy("customer_id").count
        .describe("count").collect.map(r => (r.getAs[String](0), r.getAs[String](1).toDouble)).toMap

      val scoreUDF = udf { (count: Long) =>
        math.min(math.max(((count.toDouble - 2 * baseSimilarStats("mean")) / baseSimilarStats("stddev")) + 0.6, 0.5), 0.99)
      }

      baseSimilarUsers.groupBy("customer_id")
        .count.withColumn("score", scoreUDF($"count"))
        .select("customer_id", "score")
        .join(broadcast(userBase).as("b"), Seq("customer_id"), "left_anti")
        .select("customer_id", "score").as[CustomerScore]
        .filter(_.score > scoreThreshold)
        .repartition($"customer_id")
        .rdd.sortBy(_.score, ascending = false)
        .zipWithIndex()
        .filter { case (_, idx) => idx < userLimit }
        .keys.toDF()
        .select($"customer_id".alias("CustomerId")) // Column header with this name is required for CMA injection.
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(baseSimilarUsersScoredPath)

    } else {
      Seq.empty[String].toDF("CustomerId").coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(baseSimilarUsersScoredPath)
    }
  }

}