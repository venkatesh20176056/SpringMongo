package com.paytm.map.featuresplus

import com.paytm.map.features.utils.UDFs.gaugeValuesWithTags
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame

object ModelUtils {

  def publishModelMetrics(
    modelName: String,
    modelVersion: String,
    modelMetric: String,
    value: Double
  ): Unit = {
    val metricName = s"model.$modelName"
    gaugeValuesWithTags(
      metricName,
      Map("version" -> modelVersion, "metric" -> modelMetric),
      value
    )
  }

  implicit class ModelDataFrame(df: DataFrame) {

    def assembleFeatures(featureCols: Array[String]): DataFrame = {
      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("features")

      assembler.transform(df)
    }

    def scaleFeatures(withStd: Boolean, withMean: Boolean): DataFrame = {
      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .setWithStd(withStd)
        .setWithMean(withMean)

      val scalerModel = scaler.fit(df)

      scalerModel
        .transform(df)
        .drop("features")
    }

    def trainTestSplit(trainRatio: Double): (DataFrame, DataFrame) = {
      val testRatio = 1 - trainRatio
      val splittedDF = df.randomSplit(Array(trainRatio, testRatio), seed = 42)
      (splittedDF(0), splittedDF(1))
    }

    def evaluateBinaryClassifier(metricName: String = "areaUnderROC"): Double = {
      new BinaryClassificationEvaluator()
        .setMetricName(metricName)
        .evaluate(df)
    }
  }

}