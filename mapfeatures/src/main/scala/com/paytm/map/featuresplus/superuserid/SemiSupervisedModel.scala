package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValid, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.superuserid.Constants.PathsClass
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.paytm.map.featuresplus.superuserid.Constants._
import org.apache.spark.sql.functions._
import com.paytm.map.featuresplus.ModelUtils.ModelDataFrame
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import com.paytm.map.featuresplus.superuserid.Utility._

object SemiSupervisedModel extends SemiSupervisedModelJob with SparkJobBootstrap with SparkJob

trait SemiSupervisedModelJob {
  this: SparkJob =>

  val JobName = "SemiSupervisedLearning"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    SparkJobValid
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    /** arguments */
    val chkptFlag = if (args(1) == "true") true else false
    val date = ArgsUtils.getTargetDate(args)
    val dateStr = ArgsUtils.getTargetDateStr(args)
    val Path = new PathsClass(settings.featuresDfs.featuresPlusRoot)
    val ModelConfig = new ModelConfigClass()

    /** work starts here */
    val fraudRelations = spark.read.parquet(Path.fraudData).
      withColumn("fraudRelation", lit(true))

    val latestAllRelations = Functions.getLatestDate(Path.allRelationsPath, dateStr)
    val allRelationsPath = s"${Path.allRelationsPath}/dt=$latestAllRelations"
    val allRelations = spark.read.parquet(allRelationsPath)

    val featuresCol = allRelations.columns.filter(name => name.contains(similarity))
    val inData = allRelations.select((identifierCols ++ featuresCol).map(r => col(r)): _*)

    /** getting negative labels */
    val labeledData = inData.
      join(fraudRelations, identifierCols, "left").
      withColumn(initialLabelCol, when(col("fraudRelation").isNotNull, positiveTag).otherwise(negativeTag))

    lazy val firstModelData = removeClassImbalance(labeledData, initialLabelCol)

    val firstModelDataChkpt = if (chkptFlag) {
      firstModelData.write.mode(SaveMode.Overwrite).parquet(Path.firstModelDataPath)
      spark.read.parquet(Path.firstModelDataPath)
    } else {
      firstModelData
    }.cache()

    /** initial model */
    val rfc = new RandomForestClassifier()
    val firstModel = rfc.fit(
      firstModelDataChkpt.na.fill(0L).assembleFeatures(featuresCol),
      ModelConfig.rfParamMap(rfc)
    )
    firstModelDataChkpt.unpersist()

    val firstModelChkpt = {
      import org.apache.spark.ml.classification.RandomForestClassificationModel
      firstModel.write.overwrite().save(Path.firstModelPath)
      RandomForestClassificationModel.load(Path.firstModelPath)
    }

    val firstModelPrediction = firstModelChkpt.transform(inData.na.fill(0L).assembleFeatures(featuresCol))

    val confidentLabeledData = firstModelPrediction.
      withColumn("confidence", UDF.getVectorElem(DIGIT_PRECISION, negativeTag.toInt)(col(probabCol))).
      filter((col("confidence") leq MIN_THRESHOLD) || (col("confidence") geq MAX_THRESHOLD)).
      withColumn(confidentLabelCol, when(col("confidence") leq MIN_THRESHOLD, positiveTag).otherwise(negativeTag))

    val confidentLabeledDataChkpt = {
      confidentLabeledData.write.mode(SaveMode.Overwrite).parquet(Path.confidentLabeledDataPath)
      spark.read.parquet(Path.confidentLabeledDataPath)
    } // todo checkpoint necessary

    lazy val secondModelData = removeClassImbalance(confidentLabeledDataChkpt, confidentLabelCol)

    val secondModelDataChkpt = if (chkptFlag) {
      secondModelData.write.mode(SaveMode.Overwrite).parquet(Path.secondModelDataPath)
      spark.read.parquet(Path.secondModelDataPath)
    } else {
      secondModelData
    }

    val dt = new DecisionTreeClassifier()
    val secondModel = dt.fit(secondModelDataChkpt, ModelConfig.dtParamMap(dt))

    secondModel.write.overwrite().save(s"${Path.modelPath}/dt=$dateStr")

  }

  // currently down-sample only
  def removeClassImbalance(data: DataFrame, imbalanceCol: String): DataFrame = {
    import data.sparkSession.implicits._
    val classCount = data.
      groupBy(imbalanceCol).
      agg(count(imbalanceCol).as("count")).
      orderBy(imbalanceCol).
      select("count").
      map(_.getLong(0)).collect()
    val minClassCount = classCount.min
    println(s"minClass Count = $minClassCount")
    val samplingFractions = classCount.map(r => minClassCount / r.toDouble)
    classCount.indices.map { r =>
      data.
        filter(col(imbalanceCol) === r).
        randomSplit(
          Array(samplingFractions(r), 1 - samplingFractions(r))
        ).head
    }.reduce(_ union _)
  }

}
