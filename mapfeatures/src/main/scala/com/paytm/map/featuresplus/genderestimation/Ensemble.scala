package com.paytm.map.featuresplus.genderestimation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.UDFs.standardizeGender
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.joda.time.DateTime

object Ensemble extends EnsembleJob
  with SparkJob with SparkJobBootstrap

trait EnsembleJob {
  this: SparkJob =>

  val JobName = "Ensemble"

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
    val customerToFirstNamePath = s"${featuresDfs.featuresPlusData}/customerToFirstName/"
    val firstNameToGenderPath = s"${featuresDfs.featuresPlusData}/firstNameToGender/"
    val scoredPath = s"${featuresDfs.featuresPlusModel}/gender/rf/test/"

    // Output Paths
    val ensembledPath = s"${featuresDfs.featuresPlusFeatures}/$featureName/dt=$targetDateStr/"

    // WORK
    val custToName = spark.read.parquet(customerToFirstNamePath)
    val nameToGen = spark.read.parquet(firstNameToGenderPath)
    val custToGen = custToName.join(nameToGen, Seq("first_name"), "left_outer")

    custToGen.cache

    val modelOut = spark.read.parquet(s"$scoredPath/dt=$targetDateStr")
      .select(
        $"customer_id",
        $"features",
        $"rawPrediction",
        $"probability" as "modelProbability",
        $"prediction" as "modelPrediction"
      )

    val joinedOut = modelOut.join(custToGen, Seq("customer_id")).dropDuplicates("customer_id")

    val ensembled = joinedOut
      .withColumn(
        featureName,
        when($"probability" > 0.5, $"prediction")
          .otherwise($"modelPrediction")
          .cast(IntegerType)
          .cast(StringType)
      )
      .select($"customer_id", standardizeGender(col(featureName)) as featureName)

    ensembled.saveParquet(ensembledPath, withRepartition = false)

  }
}