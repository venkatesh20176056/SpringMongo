package com.paytm.map.features.similarity.Model

import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait AffinityMethod {

  def computeLookalikeExact(spark: SparkSession, seedDF: DataFrame, universeDF: DataFrame, affinityDF: DataFrame, ConcatN: Int): DataFrame = {

    import spark.implicits._

    val firstNconcat = udf((a: Seq[String], b: Int) => {
      a.slice(0, b).mkString("_")
    })

    val universeAffinity = universeDF.join(affinityDF, Seq("customer_id"))

    val seedAffinity = seedDF.join(affinityDF, Seq("customer_id"))

    val affinityConcatDF = universeAffinity.select($"customer_id", firstNconcat($"top_cats", lit(ConcatN))
      .as("concatkey"))
      .repartition($"concatkey")

    val baseConcatKeys = seedAffinity
      .select($"customer_id", firstNconcat($"top_cats", lit(ConcatN))
        .as("concatkey"))
      .repartition($"concatkey")
      .select("concatkey")
      .distinct

    val exactMatched = affinityConcatDF.join(baseConcatKeys, Seq("concatKey")).select("customer_id").distinct()

    exactMatched // return

  }

  def computeLookalikeUsers(spark: SparkSession, baseCustomerDF: DataFrame,
    ISTdate: String, ConcatN: Int, maxNumUsers: Int = 20000): DataFrame = {

    /**
     *
     * input :
     * ISTdate: String
     * ConcatN: Int = 5
     * baseCustomerDF:DataFrame - customer_id:Integer
     *
     * output:
     * lookalikeCustomerDF:DataFrame - customer_id:Integer
     *
     *
     */

    val basePath = "s3a://midgar-aws-workspace/prod/shinra/modelling/mixer/rankedEngageCategories"

    val affinityDF = spark.read.load(basePath + "/" + ISTdate)

    val seedDF = baseCustomerDF.select("customer_id").distinct()

    val universeDF = affinityDF.select("customer_id").except(seedDF)

    val lookalikeDF = computeLookalikeExact(spark, seedDF, universeDF, affinityDF, ConcatN)
      .limit(maxNumUsers)
      .select("customer_id")

    lookalikeDF //return

  }

  def ComputeAndWriteSimilarUsers(spark: SparkSession, settings: Settings,
    userBaseDF: DataFrame, similarUsersOutputPath: String,
    sharedOutputPath: String, sharedOutputSuffix: String,
    executionOutputPath: String,
    targetDateStr: String, startDateStr: String, maxNumUsers: Int, concatN: Int) {
    import spark.implicits._

    val dateForLoadingData = ArgsUtils.formatter.parseDateTime(targetDateStr)
      .minusDays(1).toString(ArgsUtils.formatter)
    val lookalikeUser = computeLookalikeUsers(spark, userBaseDF, dateForLoadingData, concatN, maxNumUsers = maxNumUsers)
    lookalikeUser
      .select($"customer_id" as "CustomerId")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite).option("header", "true")
      .csv(similarUsersOutputPath)
  }
}
