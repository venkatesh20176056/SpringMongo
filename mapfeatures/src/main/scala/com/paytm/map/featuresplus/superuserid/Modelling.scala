package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.superuserid.Constants.{PathsClass, compulsoryFeature, identifierCols, _}
import com.paytm.map.featuresplus.superuserid.Utility.Functions
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import com.paytm.map.featuresplus.ModelUtils.ModelDataFrame

object Modelling extends ModellingJob with SparkJobBootstrap with SparkJob

trait ModellingJob {
  this: SparkJob =>

  val JobName = "ModellingRelations"

  /**
   * Model all relations to get only the significant relation
   */

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    /** arguments **/
    val date = ArgsUtils.getTargetDate(args)
    val dateStr = ArgsUtils.getTargetDateStr(args)
    val Path = new PathsClass(settings.featuresDfs.featuresPlusRoot)

    /** Work is here **/
    val allRelations = spark.read.parquet(s"${Path.allRelationsPath}/dt=$dateStr")

    val featuresCol = allRelations.columns.filter(name => name.contains(similarity))

    val modelAvailableDate = Functions.getLatestDate(Path.modelPath, dateStr)
    val model = DecisionTreeClassificationModel.load(s"${Path.modelPath}/dt=$modelAvailableDate")

    val inData = allRelations.na.fill(0L).
      filter(compulsoryFeature.reduce(_ && _)).
      select((identifierCols ++ featuresCol).map(r => col(r)): _*).
      assembleFeatures(featuresCol)

    val modelledRelations = model.transform(inData).
      filter(col(similarCol) === positiveTag)

    modelledRelations.write.mode(SaveMode.Overwrite).parquet(s"${Path.modelledRelationPath}/dt=$dateStr")

  }

}
