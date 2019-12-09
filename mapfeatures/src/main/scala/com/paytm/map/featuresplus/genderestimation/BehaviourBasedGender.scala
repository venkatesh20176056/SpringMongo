package com.paytm.map.featuresplus.genderestimation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.FileUtils._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.ModelUtils._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object BehaviourBasedGender extends BehaviourBasedGenderJob
  with SparkJob with SparkJobBootstrap

trait BehaviourBasedGenderJob {
  this: SparkJob =>

  val JobName = "BehaviourBasedGender"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import GEModelConfigs._
    import settings._
    import spark.implicits._

    // Input Args and Paths
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr: String = targetDate.toString(ArgsUtils.formatter)
    val trainingIntervalDays: Int = args(1).toInt
    val flatTablePath = s"${featuresDfs.featuresTable}/1/${featuresDfs.joined}"

    // Output Paths
    val modelPath = s"${featuresDfs.featuresPlusModel}/gender/rf/savedModel/"
    val trainPath = s"${featuresDfs.featuresPlusModel}/gender/rf/train/"
    val testPath = s"${featuresDfs.featuresPlusModel}/gender/rf/test/"

    val latestFlatTableDate = getLatestTableDate(flatTablePath, spark, targetDateStr, "dt=").orNull

    val dfRaw = spark.read.parquet(s"$flatTablePath/dt=$latestFlatTableDate")
    val ec2Features = getEC2Features(dfRaw.columns)
    val allCols = otherFeatures ++ ec2Features

    val df = dfRaw.select(allCols.head, allCols.tail: _*)
    val featureCols = df.columns.diff(Seq("customer_id", "gender"))
    val relevantDF = df.na.drop(2).na.fill(0)

    relevantDF.cache

    val scaledDF = relevantDF
      .assembleFeatures(featureCols)
      .scaleFeatures(withStd = true, withMean = true)
      .withColumnRenamed("scaledFeatures", "features")
      .withColumn(
        "label",
        when($"gender" === "male", maleLabel)
          .otherwise(when($"gender" === "female", femaleLabel))
      )

    val labeledDF = scaledDF.filter($"label".isNotNull)
    val testDF = scaledDF.filter($"label".isNull)

    val latestModelDate = getLatestTableDate(modelPath, spark, targetDateStr, "dt=", terminalString = "")
    val trainDay = if (latestModelDate.isEmpty) true
    else targetDate.minusDays(trainingIntervalDays).toString(ArgsUtils.formatter) == latestModelDate.get

    val rfModel = if (trainDay) {

      val femaleDF = labeledDF.filter($"label" === femaleLabel)
      val femaleCount = femaleDF.count.toInt
      val maleDF = labeledDF.filter($"label" === maleLabel).limit(femaleCount)

      val trainBalancedDF = femaleDF.union(maleDF).repartition(500)
      val (trainDF, validationDF) = trainBalancedDF.trainTestSplit(trainRatio = 0.8)

      trainDF.cache

      val rf = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(100)
        .setMaxDepth(10)

      // TRAIN
      val rfModel = rf.fit(trainDF)

      // Evaluate and Publish Metrics
      val transformedDF = rfModel
        .transform(validationDF)

      val areaUnderROC = transformedDF
        .evaluateBinaryClassifier()

      val areaUnderPR = transformedDF
        .evaluateBinaryClassifier("areaUnderPR")

      publishModelMetrics(
        modelName    = modelName,
        modelVersion = "RF",
        modelMetric  = "areaUnderROC",
        value        = areaUnderROC
      )

      publishModelMetrics(
        modelName    = modelName,
        modelVersion = "RF",
        modelMetric  = "areaUnderPR",
        value        = areaUnderPR
      )

      // Save everything
      rfModel.save(s"$modelPath/dt=$targetDateStr")
      trainDF.saveParquet(s"$trainPath/dt=$targetDateStr", withRepartition = false)

      rfModel
    } else {
      RandomForestClassificationModel.load(s"$modelPath/dt=${latestModelDate.get}")
    }
    rfModel.transform(testDF).saveParquet(s"$testPath/dt=$targetDateStr", withRepartition = false)
  }

}