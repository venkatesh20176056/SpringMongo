package com.paytm.map.featuresplus.rechargeprediction

import com.paytm.map.features.SparkJobValidations.{DailyJobValidation, ArgLengthValidation}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.rechargeprediction.Constants._
import com.paytm.map.featuresplus.rechargeprediction.Utility._
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Features extends FeaturesJob with SparkJobBootstrap with SparkJob

trait FeaturesJob {
  this: SparkJob =>

  val JobName = "RechargePredictionFeatureGeneration"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    ArgLengthValidation.validate(args, 2) && DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import spark.implicits._
    /** Date Utils **/
    val date: DateTime = ArgsUtils.getTargetDate(args)
    val trainFlag = if (args(1) == "train") true else false

    println(trainFlag)
    val lookBackDays = Constants.lookBackDays
    val dateRange = DateBound(date, lookBackDays)
    val basePath = Constants.projectBasePath(settings.featuresDfs.featuresPlusRoot)

    /** read **/

    val rawData = spark.read.parquet(s"$basePath/${Constants.rawDataPath}/date=${dateRange.endDate}")

    val baseTable = rawData.groupBy(identifierCols.map(r => col(r)) :+ col("date"): _*).
      agg(
        sum(col("recharge_amount")) as "amount",
        count(col("order_item_id")) as "count",
        sum(col("discount")) as "discount",
        count(col("is_ben_same")) as "countSelfRecharge",
        first(col("age")) as "age",
        first(col("is_email_verified")) as "is_email_verified"
      )

    val lifeTimeFeatures = baseTable.groupBy(identifierCols.map(r => col(r)): _*).
      agg(
        count($"date") as "numOfRechargeDaysLFT",
        sum($"amount") as "amountLFT",
        sum($"count") as "numOfRechargeLFT",
        sum($"countSelfRecharge") as "countSelfRechargeLFT",
        sum($"discount") as "discountLFT",
        min($"date") as "firstRechargeDate"
      ).filter($"numOfRechargeDaysLFT" > numOfRechargeThresh)

    val filteredBaseTable = baseTable.join(lifeTimeFeatures, identifierCols.toSeq)

    val window1 = Window.partitionBy(identifierCols.map(r => col(r)): _*).
      orderBy(asc("date"))

    val window2 = Window.partitionBy(identifierCols.map(r => col(r)): _*).
      orderBy(asc("date")).
      rowsBetween(Long.MinValue, 0)

    val offset: Array[Int] = if (trainFlag) Array(1, 2) else Array(0, 1)

    val DateSoFarFeatures = filteredBaseTable.
      withColumn("D0_Date", $"date").
      withColumn("D1_Date", lag("date", offset.head).over(window1)).
      withColumn("D2_Date", lag("date", offset.last).over(window1)).
      withColumn("D0_Amount", $"amount").
      withColumn("D1_Amount", lag("amount", offset.head).over(window1)).
      withColumn("D2_Amount", lag("amount", offset.last).over(window1)).
      withColumn("rank", rank.over(window1)).
      na.drop(Seq("D1_Date", "D1_amount")).
      withColumn("sumAmount", sum("D1_Amount").over(window2)).
      withColumn("avgAmount", avg("D1_Amount").over(window2)).
      withColumn("stddevAmount", stddev("D1_Amount").over(window2)).
      na.drop(Seq("D2_Date")).
      withColumn("D12_DeltaDays", UDF.getTimeDelta(col("D2_Date"), col("D1_Date"))).
      withColumn("D1&_DeltaDays", UDF.getTimeDelta(col("firstRechargeDate"), col("D1_Date"))).
      withColumn("D2_ConsumptionSlope", $"D2_Amount" / $"D12_DeltaDays").
      withColumn("D&_ConsumptionSlope", $"sumAmount" / $"D1&_DeltaDays").
      withColumn("avgD12_DeltaDays", avg("D12_DeltaDays").over(window2)).
      withColumn("stddevD12_DeltaDays", stddev("D12_DeltaDays").over(window2)).
      withColumn("avgD2_ConsumptionSlope", avg("D2_ConsumptionSlope").over(window2)).
      withColumn("stddevD2_ConsumptionSlope", stddev("D2_ConsumptionSlope").over(window2)).
      na.drop().
      drop(Seq("amount", "date"): _*)

    if (trainFlag) {
      println(s"$basePath/${Constants.trainFeaturesPath}/date=${dateRange.endDate}")
      DateSoFarFeatures.
        withColumn("D01_DeltaDays", UDF.getTimeDelta(col("D1_Date"), col("D0_Date"))).
        write.mode(SaveMode.Overwrite).
        parquet(s"$basePath/${Constants.trainFeaturesPath}/date=${dateRange.endDate}")
    } else {
      println(s"$basePath/${Constants.testFeaturesPath}/date=${dateRange.endDate}")
      DateSoFarFeatures.
        filter(col("numOfRechargeDaysLFT") === col("rank")).
        write.mode(SaveMode.Overwrite).
        parquet(s"$basePath/${Constants.testFeaturesPath}/date=${dateRange.endDate}")
    }
  }

  // TODO: changed copy the latest one
  """
    |root
    | |-- ben_phone_no: string (nullable = true)
    | |-- operator: string (nullable = true)
    | |-- circle: string (nullable = true)
    | |-- count: long (nullable = true)
    | |-- discount: double (nullable = true)
    | |-- countSelfRecharge: long (nullable = true)
    | |-- age: integer (nullable = true)
    | |-- is_email_verified: integer (nullable = true)
    | |-- numOfRecharge: long (nullable = true)
    | |-- firstRechargeDate: date (nullable = true)
    | |-- D0_Date: date (nullable = true)
    | |-- D1_Date: date (nullable = true)
    | |-- D2_Date: date (nullable = true)
    | |-- D0_Amount: double (nullable = true)
    | |-- D1_Amount: double (nullable = true)
    | |-- D2_Amount: double (nullable = true)
    | |-- rank: integer (nullable = true)
    | |-- sumAmount: double (nullable = true)
    | |-- avgAmount: double (nullable = true)
    | |-- stddevAmount: double (nullable = true)
    | |-- D01_DeltaDays: integer (nullable = true)
    | |-- D12_DeltaDays: integer (nullable = true)
    | |-- D1&_DeltaDays: integer (nullable = true)
    | |-- D2_ConsumptionSlope: double (nullable = true)
    | |-- D&_ConsumptionSlope: double (nullable = true)
    | |-- avgD12_DeltaDays: double (nullable = true)
    | |-- stddevD12_DeltaDays: double (nullable = true)
    | |-- avgD2_ConsumptionSlope: double (nullable = true)
    | |-- stddevD2_ConsumptionSlope: double (nullable = true)
  """.stripMargin

}
