package com.paytm.map.featuresplus.rechargeprediction

import java.sql.Timestamp

object Constants {
  case class AgeObject(min: Int, max: Int)
  val ageLimit = AgeObject(18, 118)
  val ageMissingEncoding = 0

  case class OperatorPredRechDate(BenPhoneNo: String, Operator: String, Circle: String, PredictedRechDate: Timestamp)

  val lookBack = 6
  val lookBackDays: Int = lookBack * 30

  val identifierCols = Array("ben_phone_no", "operator", "circle")
  val numOfRechargeThresh: Int = lookBack

  /** Paths utils **/

  def profilePath(profilePath: String) = profilePath.replace("/stg/", "/prod/")
  def projectBasePath(rootPath: String) = s"$rootPath$packageName"
  val packageName = "rechargeprediction"

  val partitionDataPath = s"partitionData"
  val rawDataPath = s"rawData"
  val baseTablePath = s"baseTable"
  val trainFeaturesPath = s"trainFeatures"
  val testFeaturesPath = s"testFeatures"
  val stringIndexPath = s"stringIndex"
  val pipelineModelPath = s"pipelineModel"
  val scoredDataPath = s"scoredData"

  // for debugging
  val trainDataPredictionPath = s"trainDataPrediction"
  val testDataPredictionPath = s"testDataPrediction"

  // for classification + multiple regression model only
  val classifyPipelineModelPath = s"classifyPipelineModel"
  val classifyTrainDataPredictionPath = s"classifyTrainDataPrediction"
  val classifyTestDataPredictionPath = s"classifyTestDataPrediction"

  val regPipelineModel1Path = s"regPipelineModel/model1"
  val regPipelineModel2Path = s"regPipelineModel/model2"

  val regData1PredictionPath = s"regDataPrediction/model1"
  val regData2PredictionPath = s"regDataPrediction/model2"

  def finalDataPath(rootPath: String) = s"$rootPath/$packageName"

  /** columns Names **/
  val dvCol = "D01_DeltaDays"
  val featuresCol = "features"
  val regLabelCol = "regLabel"
  val operatorCol = "operator"
  val circleCol = "circle"
  val nextRechargeDateCol = "nextRechargeDate"
  val rechargePredictionOutputCol = "pred_rech_dates"

  /** Model Params **/
  // params from grid search
  case class regressionModelParamClass() {
    val numTrees = 100
    val maxDepth = 10
    val featureSubsetStrg = "onethird"
    val subSamplingRate = 0.25
    val PredictionCol = "regPrediction"
  }
  val RegressionModelParam = regressionModelParamClass()

}
