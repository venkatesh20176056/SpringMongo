package com.paytm.map.featuresplus.rechargeprediction

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValid, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import com.paytm.map.featuresplus.rechargeprediction.Utility._

object RegressorTraining extends RegressorTrainingJob with SparkJob with SparkJobBootstrap

trait RegressorTrainingJob {
  this: SparkJob =>

  val JobName = "RechargePredictionRegressorTraining"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args) && DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import spark.implicits._

    /** Date Utils **/
    val date: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays = Constants.lookBackDays
    val debugModeEnabled = if (args(1) == "true") true else false
    val dateRange = DateBound(date, lookBackDays)
    val basePath = Constants.projectBasePath(settings.featuresDfs.featuresPlusRoot)

    /** read **/
    val featuresData = spark.read.parquet(s"$basePath/${Constants.trainFeaturesPath}/date=${dateRange.endDate}")

    /** handling categorical features **/
    val operatorCol = "operator"
    val circleCol = "circle"
    val operatorValues = featuresData.na.drop(Seq(operatorCol)).select(col(operatorCol).cast(StringType)).distinct.map(_.getString(0)).collect()
    val circleValues = featuresData.na.drop(Seq(circleCol)).select(col(circleCol).cast(StringType)).distinct.map(_.getString(0)).collect()
    val toAugmentColumns = operatorValues ++ circleValues

    def getCategoryIndex(expectedValue: String) = udf((operatorValue: String, circleValue: String) =>
      if (operatorValue == expectedValue || circleValue == expectedValue) 1 else 0)

    val nonCategoricalDF: DataFrame = toAugmentColumns.toSeq.foldLeft[DataFrame](featuresData)(
      (acc, c) =>
        acc.withColumn(c, getCategoryIndex(c)($"operator", $"circle"))
    )

    /** preparing for the model **/
    val idv: Seq[String] = nonCategoricalDF.schema.
      filter(r => !(r.dataType == StringType || r.dataType == DateType)).
      filter(r => (r.name != Constants.dvCol) && !r.name.contains("D0") && r.name != "rank").
      map(r => r.name)

    val selectedColumns: Seq[Column] = if (debugModeEnabled) {
      col(Constants.featuresCol) +: col(Constants.dvCol).as(Constants.regLabelCol) +: nonCategoricalDF.columns.map(r => col(r))
    } else {
      col(Constants.featuresCol) +: col(Constants.dvCol).as(Constants.regLabelCol) +: Seq()
    }

    val assembled = new VectorAssembler().
      setInputCols(idv.toArray).
      setOutputCol(Constants.featuresCol).
      transform(
        nonCategoricalDF.na.drop()
      ).select(selectedColumns: _*)

    val Array(trainData, testData) = if (debugModeEnabled)
      assembled.randomSplit(Array(0.9, 0.1)) else Array(assembled, null)
    trainData.persist(StorageLevel.MEMORY_ONLY_2)

    /** modelling **/
    val regressor = new RandomForestRegressor().
      setLabelCol(Constants.regLabelCol).
      setFeaturesCol(Constants.featuresCol).
      setPredictionCol(Constants.RegressionModelParam.PredictionCol).
      setNumTrees(Constants.RegressionModelParam.numTrees).
      setMaxDepth(Constants.RegressionModelParam.maxDepth).
      setFeatureSubsetStrategy(Constants.RegressionModelParam.featureSubsetStrg).
      setSubsamplingRate(Constants.RegressionModelParam.subSamplingRate)

    val pipelineModel = regressor.fit(trainData)

    /** saving everything **/
    println(s"$basePath/${Constants.stringIndexPath}/date=${dateRange.endDate}")
    toAugmentColumns.
      toSeq.zipWithIndex.
      toDF("values", "index").
      write.mode(SaveMode.Overwrite).
      parquet(s"$basePath/${Constants.stringIndexPath}/date=${dateRange.endDate}")

    println(s"$basePath/${Constants.pipelineModelPath}/date=${dateRange.endDate}")
    pipelineModel.
      asInstanceOf[RandomForestRegressionModel].write.overwrite().
      save(s"$basePath/${Constants.pipelineModelPath}/date=${dateRange.endDate}")

    /** further for debugging only **/
    if (debugModeEnabled) {
      testData.persist(StorageLevel.MEMORY_ONLY_2)
      val dataMap = Map(trainData -> "trainData", testData -> "testData")

      dataMap.keys.foreach { dataset =>
        val prediction = pipelineModel.transform(dataset)

        /** evaluation **/
        Array("rmse", "mae").foreach { metric =>
          val evaluation = new RegressionEvaluator()
            .setLabelCol(Constants.regLabelCol)
            .setPredictionCol(Constants.RegressionModelParam.PredictionCol)
            .setMetricName(metric)
          println(s" Performance Metrics: $metric for ${dataMap.getOrElse(dataset, "notFound")} is ${evaluation.evaluate(prediction)}")
        }

        /** save for further evaluation **/
        println(s"$basePath/${dataMap.getOrElse(dataset, "notFound")}Prediction/date=${dateRange.endDate}")
        prediction.write.mode(SaveMode.Overwrite).
          parquet(s"$basePath/${dataMap.getOrElse(dataset, "notFound")}Prediction/date=${dateRange.endDate}")
      }

    }

  }

}