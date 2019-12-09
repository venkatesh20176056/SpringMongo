package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.baseDataPrep
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions.{lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object EvalSeedAndUniverseOffline extends EvalSeedAndUniverse {

  private def dataPreparation(
    spark: SparkSession,
    date: DateTime, settings: Settings, excludedSegment: Option[DataFrame]
  ): Map[String, DataFrame] = {

    val offlineCols = Seq(
      "customer_id",
      "OFFLINE_total_transaction_count",
      "OFFLINE_first_transaction_date"
    )

    val rawDF = baseDataPrep(offlineCols, date, settings)(spark).orderBy(rand())

    val baseDF = excludedSegment match { // to remove some specific group, such as customers only having clicks
      case Some(x) => rawDF.join(x, Seq("customer_id"), "left_anti")
      case _       => rawDF
    }

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._
    val seedALLDF = baseDF.withColumn("today", lit(date.toString(ArgsUtils.formatter)))
      .withColumn("daysAfterFirstOffline", datediff($"today", $"OFFLINE_first_transaction_date"))
      .where($"daysAfterFirstOffline".lt(90))
      .select($"customer_id".cast(LongType) as "customer_id") //inner join to meet both conditions

    val negativeALLDF = baseDF.where(col("OFFLINE_total_transaction_count").equalTo(0)).select("customer_id")

    val dfMap = Map("seedAll" -> seedALLDF, "negativeAll" -> negativeALLDF)

    dfMap //return

  }

  override def generate(
    spark: SparkSession,
    date: DateTime,
    settings: Settings,
    trainingData: String,
    baseData: String,
    testingData: String,
    positiveSampleRatio: Double,
    excludedSegment: Option[DataFrame]
  ): Map[String, DataFrame] = {

    val baseData = dataPreparation(spark, date, settings, excludedSegment)

    val seedAllDF = dfOrError(baseData, "seedAll")
    val negativeAllDF = dfOrError(baseData, "negativeAll")

    val result = generate(spark, seedAllDF, negativeAllDF, positiveSampleRatio)

    result("universe").coalesce(1).write.mode("overwrite").parquet(testingData)
    result("seed").coalesce(1).write.mode("overwrite").parquet(trainingData)

    result

  }

  def generate(
    spark: SparkSession,
    seedAllDF: DataFrame,
    negativeAllDF: DataFrame,
    positiveSampleRatio: Double
  ): Map[String, DataFrame] = {

    val seedDF = seedAllDF.sample(withReplacement = false, positiveSampleRatio).cache()
    val seedRemainingDF = seedAllDF.except(seedDF)
    val positiveSampleSize = seedRemainingDF.count() // fixed size, easy to be tested
    val negativeAllDFSize = negativeAllDF.count()
    val sampleSize = math.min(positiveSampleSize, negativeAllDFSize)
    val positiveSample = if (positiveSampleSize == sampleSize) {
      seedRemainingDF.withColumn("label", lit(1))
    } else {
      seedRemainingDF.sample(withReplacement = false, 1D * sampleSize / positiveSampleSize).withColumn("label", lit(1)).cache()
    }

    //val negSampleSize = math.min(4 * sampleSize, negativeAllDFSize)
    val negativeSample = negativeAllDF.limit(sampleSize.toInt).withColumn("label", lit(0)).cache()

    val universeSample = positiveSample.union(negativeSample).orderBy(rand()).cache()
    universeSample.count()

    val dfMap = Map("seed" -> seedDF, "universe" -> universeSample)
    dfMap //return

  }
}
