package com.paytm.map.features.similarity.Model

import com.paytm.map.features.similarity.{logger, num_hashtables, num_partitions}
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

trait LSHMethod extends Serializable {

  private def to_sparse(arr: Seq[Int], max_categories: Int): Vector = {
    val indices = arr.toArray.sorted
    val len = indices.length
    val values = Array.fill(len) {
      1.0
    }
    new SparseVector(max_categories, indices, values)
  }

  private def computeKendall(arrA: Seq[Int], arrB: Seq[Int]): Double = {
    val x: Array[Double] = arrA.map(_.toDouble).toArray
    val y: Array[Double] = arrB.map(_.toDouble).toArray
    val kc = new KendallsCorrelation
    kc.correlation(x, y)
  }

  private val to_sparseUDF: UserDefinedFunction = udf[Vector, Seq[Int], Int](to_sparse)

  private val computeKendallUDF: UserDefinedFunction = udf[Double, Seq[Int], Seq[Int]](computeKendall)

  private val sliceUDF: UserDefinedFunction = udf((array: Seq[Int], from: Int, to: Int) => array.slice(from, to))

  private val stringValUDF: UserDefinedFunction = udf((array: Seq[Int]) => array.mkString("_"))

  private val maxCategoriesUDF: UserDefinedFunction = udf((value: Seq[Int]) => value.max)

  /**
   * Builds minHashLSH model with Jaccard distance. Kendall Tau is used for rank correlation to return
   * the final neighbors
   *
   * @param seedDF     seed dataFrame
   * @param universeDF universe dataFrame -> search space for neighbors
   * @param spark      sparkSession
   * @return neighbor dataFrame
   */
  def buildLSHModel(seedDF: DataFrame, universeDF: DataFrame, distanceThreshold: Double)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val lsh = new MinHashLSH().setNumHashTables(num_hashtables).setInputCol("top_cats_sparse").setOutputCol("hashes")
    val lshModel = lsh.fit(seedDF)
    val transformedSeedDF = lshModel.transform(seedDF)
    val transformedUniverseDF = lshModel.transform(universeDF)

    val uniqueTransformedSeedDF = transformedSeedDF
      .select("top_n_cats", "top_n_cats_str", "top_cats_sparse", "hashes")
      .distinct()
    val uniqueTransformedUniverseDF = transformedUniverseDF
      .select("top_n_cats", "top_n_cats_str", "top_cats_sparse", "hashes")
      .distinct()

    val approxJoin = lshModel
      .approxSimilarityJoin(
        uniqueTransformedSeedDF,
        uniqueTransformedUniverseDF, distanceThreshold, "JaccardDistance"
      ).select(
        $"datasetA.top_n_cats".as("seed_cats"),
        $"datasetB.top_n_cats".as("neighbor_cats"),
        $"datasetB.top_n_cats_str".as("top_n_cats_str"),
        $"JaccardDistance"
      ).repartition(num_partitions, $"top_n_cats_str")

    approxJoin

  }

  /**
   * *
   * Compute KendallScore for input transformed data
   *
   * @param df    input df with seed and neighbor cats
   * @param spark implicit spark session
   * @return df with computed Kendall score over seed_cats and neighbor_cats
   */
  def computeKendallScore(df: DataFrame, kendallThreshold: Double)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val kendallDF = df
      .withColumn(
        "KendallTau",
        computeKendallUDF(df("seed_cats"), df("neighbor_cats"))
      )
      .drop("seed_cats", "neighbor_cats")
      .filter($"KendallTau" > kendallThreshold)

    kendallDF

  }

  /**
   * Create sparse representation for top n category.
   *
   * @param df    input dataFrame for transform
   * @param spark implicit sparkSession
   * @return sprase feature dataFrame
   */
  def prepareSparseDF(df: DataFrame, concatN: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val tempDF = df.filter(size($"top_cats") > 0)
      .withColumn("top_cats", $"top_cats".cast(ArrayType(IntegerType)))

    val maxCategories = tempDF
      .withColumn("max_value", maxCategoriesUDF($"top_cats"))
      .agg(max($"max_value")).first()(0)

    val sparseDF = tempDF
      .withColumn("len_cats", size($"top_cats"))
      .filter($"len_cats" >= concatN)
      .withColumn("top_n_cats", sliceUDF($"top_cats", lit(0), lit(concatN)))
      .withColumn("top_n_cats_str", stringValUDF($"top_n_cats"))
      .withColumn("top_cats_sparse", to_sparseUDF($"top_n_cats", lit(maxCategories) + 1))
      .drop("len_cats")
      .repartition(num_partitions, $"top_n_cats_str")

    sparseDF
  }

  /**
   * Driver to compute LSH based neighbors
   *
   * @param affinityDF affinity dataFrame -> output of mixer model
   * @param seedDF     seed dataFrame
   * @param universeDF universe dataFrame
   * @param spark      implicit sparkSession
   * @return neighbors dataFrame
   */
  def computeLookalikeLSH(
    affinityDF: DataFrame,
    seedDF: DataFrame,
    universeDF: DataFrame,
    concatN: Int,
    distanceThreshold: Double,
    KendallThreshold: Double
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val seedAffinityDF = affinityDF.join(seedDF, Seq("customer_id"))
    val universeAffinityDF = affinityDF.join(universeDF, Seq("customer_id"))
    val universeExcludingSeedDF = universeAffinityDF.except(seedAffinityDF).distinct().toDF

    logger.info("Building the sparse data")
    val seedSparseDF = prepareSparseDF(seedAffinityDF, concatN)
    val universeSparseDF = prepareSparseDF(universeExcludingSeedDF, concatN)

    logger.info("Building LSH model")
    val approxDF = buildLSHModel(seedSparseDF, universeSparseDF, distanceThreshold)
    val kendallScoredDF = computeKendallScore(approxDF, KendallThreshold)
    val transformedNeighborDF = universeSparseDF
      .join(kendallScoredDF, Seq("top_n_cats_str"))
      .select($"customer_id".as("CustomerId"), $"KendallTau")
      .groupBy($"CustomerId")
      .agg(max($"KendallTau").as("score"))
      .orderBy(desc("score"))

    transformedNeighborDF

  }

  /**
   * seedDF and univerDF have already joined with feature vectors
   */
  def computeLookalikeLSHWithFeatures(
    seedDF: DataFrame,
    universeDF: DataFrame,
    concatN: Int,
    distanceThreshold: Double,
    KendallThreshold: Double,
    pooling: String
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val universeExcludingSeedDF = universeDF.except(seedDF).distinct().toDF

    logger.info("Building the sparse data")
    val seedSparseDF = prepareSparseDF(seedDF, concatN)
    val universeSparseDF = prepareSparseDF(universeExcludingSeedDF, concatN)

    logger.info("Building LSH model")
    val approxDF = buildLSHModel(seedSparseDF, universeSparseDF, distanceThreshold)
    val kendallScoredDF = computeKendallScore(approxDF, KendallThreshold)
    val transformedNeighborDF: DataFrame = if (pooling.equalsIgnoreCase("mean")) {
      universeSparseDF
        .join(kendallScoredDF, Seq("top_n_cats_str"))
        .select($"customer_id".as("CustomerId"), $"KendallTau")
        .groupBy($"CustomerId")
        .agg(mean($"KendallTau").as("score"))
        .orderBy(desc("score"))
    } else {
      universeSparseDF
        .join(kendallScoredDF, Seq("top_n_cats_str"))
        .select($"customer_id".as("CustomerId"), $"KendallTau")
        .groupBy($"CustomerId")
        .agg(max($"KendallTau").as("score"))
        .orderBy(desc("score"))
    }

    transformedNeighborDF

  }

}
