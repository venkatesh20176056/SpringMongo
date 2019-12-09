package com.paytm.map.featuresplus.rechargeprediction

import java.sql.Timestamp

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.featuresplus.rechargeprediction.Utility.DateBound
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import com.paytm.map.featuresplus.rechargeprediction.Constants.OperatorPredRechDate
import com.paytm.map.featuresplus.rechargeprediction.Utility.UDF

object ExportForEngage extends ExportForEngageJob with SparkJobBootstrap with SparkJob

trait ExportForEngageJob {

  val JobName = "RechargePredictionExportForEngage"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    /** Date/ Path Utils **/
    val date: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays = Constants.lookBackDays
    val dateRange = DateBound(date, lookBackDays)
    val basePath = Constants.projectBasePath(settings.featuresDfs.featuresPlusRoot)
    val exportPath = Constants.finalDataPath(settings.featuresDfs.featuresPlusFeatures)

    /** read **/
    val scoredData = spark.read.
      parquet(s"$basePath/${Constants.scoredDataPath}/date=${dateRange.endDate}").
      select(Constants.identifierCols.map(r => col(r)) :+ col(Constants.RegressionModelParam.PredictionCol) :+ col("D0_date"): _*)

    val forecastData = scoredData.
      withColumn(
        Constants.nextRechargeDateCol,
        UDF.addDays(col(Constants.RegressionModelParam.PredictionCol), col("D0_date"))
      )

    val rawData =
      spark.read.parquet(s"$basePath/${Constants.rawDataPath}/date=${dateRange.endDate}")

    val identifiersCustomerIdMapping = rollUpBeneficiaryToCustomerID(rawData)

    val forecastAtCIDLevel = forecastData.
      join(identifiersCustomerIdMapping, Seq(Constants.identifierCols: _*), "inner")

    /** reformatting as per engage **/
    val rechargeOutput = forecastAtCIDLevel.
      withColumn(
        Constants.rechargePredictionOutputCol,
        toOutputObject(
          col(Constants.identifierCols(0)), col(Constants.identifierCols(1)), col(Constants.identifierCols(2)), col(Constants.nextRechargeDateCol)
        )
      ).
        select(col("customer_id"), col(Constants.rechargePredictionOutputCol)).
        groupBy("customer_id").
        agg(collect_set(col(Constants.rechargePredictionOutputCol)).as(Constants.rechargePredictionOutputCol))

    println(s"$exportPath/dt=${dateRange.endDate}")
    rechargeOutput.write.mode(SaveMode.Overwrite).parquet(s"$exportPath/dt=${dateRange.endDate}")

  }

  def rollUpBeneficiaryToCustomerID(data: DataFrame): DataFrame = {
    data.groupBy((Constants.identifierCols :+ "customer_id").map(r => col(r)): _*).
      agg(
        count("created_at").as("frequency"),
        max("created_at").as("lastActivity")
      ).
        withColumn("rank", rank.over(
          Window.partitionBy(Constants.identifierCols.map(r => col(r)): _*).orderBy(desc("lastActivity"))
        )).
        filter(col("rank") === 1).
        select((Constants.identifierCols :+ "customer_id").map(r => col(r)): _*).distinct()
  }

  def toOutputObject: UserDefinedFunction = udf[OperatorPredRechDate, String, String, String, Timestamp](
    (beneficiaryPhoneNo: String, operator: String, circle: String, predictedDate: Timestamp) =>
      OperatorPredRechDate(beneficiaryPhoneNo, operator, circle, predictedDate)
  )

}
