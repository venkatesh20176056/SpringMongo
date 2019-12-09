package com.paytm.map.featuresplus.rechargeprediction

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.RandomForestRegressionModel
import com.paytm.map.featuresplus.rechargeprediction.Utility._

object RegressorScoring extends RegressorScoringJob with SparkJob with SparkJobBootstrap

trait RegressorScoringJob {
  this: SparkJob =>

  val JobName = "RechargePredictionRegressorTraining"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args) && DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import spark.implicits._

    /** Date/ Path Utils **/
    val date: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays = Constants.lookBackDays
    val debugModeEnabled = if (args(1) == "true") true else false
    val dateRange = DateBound(date, lookBackDays)
    val basePath = Constants.projectBasePath(settings.featuresDfs.featuresPlusRoot)

    /** read **/
    val featuresData = spark.read.parquet(s"$basePath/${Constants.testFeaturesPath}/date=${dateRange.endDate}")

    /** handling categorical features **/
    val latestAvailableIndexer: String =
      Function.getLatest(s"$basePath/${Constants.stringIndexPath}", dateRange.endDate)
    println(s"latestDate: $latestAvailableIndexer")

    val toAugmentColumns: Seq[String] = spark.read.
      parquet(latestAvailableIndexer).
      orderBy("index").select(col("values").cast(StringType)).
      map(_.getString(0)).collect().toSeq

    def getCategoryIndex(expectedValue: String) = udf((operatorValue: String, circleValue: String) =>
      if (operatorValue == expectedValue || circleValue == expectedValue) 1 else 0)

    val nonCategoricalDF: DataFrame = toAugmentColumns.foldLeft[DataFrame](featuresData)(
      (acc, c) =>
        acc.withColumn(c, getCategoryIndex(c)(col(Constants.operatorCol), col(Constants.circleCol)))
    )

    /** preparing data for the model **/
    val idv: Seq[String] = nonCategoricalDF.schema.
      filter(r => !(r.dataType == StringType || r.dataType == DateType)).
      filter(r => (r.name != Constants.dvCol) && !r.name.contains("D0") && r.name != "rank").
      map(r => r.name)

    val selectedColumns: Array[Column] = (if (debugModeEnabled) {
      (Constants.identifierCols :+ Constants.featuresCol) ++ nonCategoricalDF.columns
    } else {
      (Constants.identifierCols :+ Constants.featuresCol) ++ Seq("D0_date")
    }).map(r => col(r))

    val assembled = new VectorAssembler().
      setInputCols(idv.toArray).
      setOutputCol(Constants.featuresCol).
      transform(
        nonCategoricalDF.na.drop()
      ).select(selectedColumns: _*)

    val scoringData = assembled

    /** loading model and scoring **/

    val latestAvailableModel: String =
      Function.getLatest(s"$basePath/${Constants.pipelineModelPath}", dateRange.endDate)
    println(s"latestDate: $latestAvailableModel")

    val pipelineModel = RandomForestRegressionModel.
      load(latestAvailableModel)
    val scoringPrediction = pipelineModel.transform(scoringData)

    /** saving everything **/
    println(s"$basePath/${Constants.scoredDataPath}/date=${dateRange.endDate}")
    scoringPrediction.
      write.mode(SaveMode.Overwrite).
      parquet(s"$basePath/${Constants.scoredDataPath}/date=${dateRange.endDate}")
  }

}