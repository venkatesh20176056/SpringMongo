package com.paytm.map.features.similarity.Model

import com.paytm.map.features.similarity.{computeKendallUDF, num_hashtables, num_partitions}
import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.sql.DataFrame

class LSHModel extends SimilarUserModels with Serializable {

  override type T = MinHashLSHModel

  /**
   * Creates a new instance of MinHashLSH
   * @return MinHashLSH instance with default params
   */
  private def defineModel: MinHashLSH = {
    new MinHashLSH().setNumHashTables(num_hashtables).setInputCol("features_sparse").setOutputCol("hashes")
  }

  /**
   * *
   * Compute KendallScore for input transformed data
   *
   * @param df Input df with seed and neighbor cats
   * @return DataFrame with computed Kendall score over seed_cats and neighbor_cats
   */
  def computeKendallScore(df: DataFrame, kendallThreshold: Double): DataFrame = {

    import df.sparkSession.implicits._

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
   *
   * @param modelPath         Model s3 path as String
   * @param seedDF            Seed DataFrame
   * @param universeDF        Universe DataFrame
   * @param distanceThreshold Distance threshold for Jaccard
   * @return DataFrame[seed_cats, neighbor_cats, features_n_str, JaccardDistance]
   */

  def computeSimilarUsers(modelPath: String, seedDF: DataFrame, universeDF: DataFrame,
    distanceThreshold: Double): DataFrame = {

    import seedDF.sparkSession.implicits._

    val model = MinHashLSHModel.load(modelPath)

    val uniqueTransformedSeedDF = seedDF
      .select("features_n", "features_n_str", "features_sparse", "hashes")
      .distinct()

    val uniqueTransformedUniverseDF = universeDF
      .select("features_n", "features_n_str", "features_sparse", "hashes")
      .distinct()

    val approxDF = computeNeighbors(model, uniqueTransformedSeedDF, uniqueTransformedUniverseDF, distanceThreshold)
      .select(
        $"datasetA.features_n".as("seed_cats"),
        $"datasetB.features_n".as("neighbor_cats"),
        $"datasetB.features_n_str".as("features_n_str"),
        $"JaccardDistance"
      ).repartition(num_partitions, $"features_n_str")

    approxDF

  }

  /**
   *
   * @param model     LSHModel
   * @param dfA       Seed DataFrame
   * @param dfB       Universe DataFrame
   * @param threshold Distance Threshold
   * @return SimilarityJoined DataFrame
   */
  protected override def computeNeighbors(model: MinHashLSHModel, dfA: DataFrame,
    dfB: DataFrame, threshold: Double): DataFrame = {
    model.approxSimilarityJoin(dfA, dfB, threshold, "JaccardDistance").toDF
  }

  /**
   *
   * @param df DataFrame to fit the LSH Model on
   * @return MinHashLSHModel
   */
  protected override def fitModel(df: DataFrame): MinHashLSHModel = {
    defineModel.fit(df)
  }

  /**
   *
   * @param model MinHashLSHModel
   * @param df    DataFrame for transform
   * @return Transformed DataFrame
   */
  protected override def transformModel(model: MinHashLSHModel, df: DataFrame): DataFrame = {
    model.transform(df)
  }

}
