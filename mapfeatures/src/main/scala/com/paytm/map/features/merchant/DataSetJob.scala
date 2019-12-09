package com.paytm.map.features.merchant

import com.paytm.map.features.Chakki.FeatChakki.Constants.level2BaseTable
import com.paytm.map.features.Chakki.FeatChakki.{FeatExecutor, ResolveFeatures}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

object DataSetJob extends DataSet with SparkJob with SparkJobBootstrap

trait DataSet {
  this: SparkJob =>

  val JobName = "MerchantDataSet"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val levelPrefix = args(1)

    val featuresDfs = settings.featuresDfs

    val level = levelPrefix

    val baseTableBasePath = settings.featuresDfs.baseDFS.aggPath
    val baseTablePath = s"$baseTableBasePath/${level2BaseTable(level)}"
    val writePath = s"${featuresDfs.featuresTable}/${featuresDfs.level}/$level/dt=$targetDateStr"

    val SpecSheetPath = s"${settings.featuresDfs.resources}${featuresDfs.merchantSpecSheets}"
    val FeatListPath = s"${settings.featuresDfs.resources}${featuresDfs.merchantFeatureList}"

    val resolvedFeatures = ResolveFeatures(spark, FeatListPath, SpecSheetPath, level)

    val baseFeatures = (resolvedFeatures.baseFeaturesRequired ++ Set("merchant_id", "dt")).toSeq.distinct

    val baseTable = readBaseTable(spark, baseTablePath, level, targetDate)
      .select(baseFeatures.head, baseFeatures.tail: _*)
    //      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //
    //    println("baseTableCount: " + baseTable.count)

    val requestedFeat = resolvedFeatures.featureSet.toSet

    if (requestedFeat.nonEmpty) {
      // Execute Features.
      val featData = getTransformedData(Map(level -> resolvedFeatures), baseTable, targetDate)

      // Write the output
      featData
        .write.mode(SaveMode.Overwrite)
        .parquet(writePath)
    }

  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  def getTransformedData(resolvedFeatures: Map[String, ResolveFeatures], baseTable: DataFrame, readDate: DateTime): DataFrame = {

    if (resolvedFeatures.size > 1) {
      // If Multi Stage Execution
      resolvedFeatures.map {
        case (_, resolvedFeats) =>
          //Apply Features
          FeatExecutor(resolvedFeats.featureSet, baseTable, readDate, entityId = "merchant_id")
            .execute()
            .persist(StorageLevel.MEMORY_AND_DISK)
      }.toSeq.joinAllWithoutRepartiton(Seq("merchant_id"), "outer")
    } else {
      // Apply Features
      val (_, resolvedFeats) = resolvedFeatures.head
      FeatExecutor(resolvedFeats.featureSet, baseTable, readDate, entityId = "merchant_id").execute()
    }

  }

  def readBaseTable(spark: SparkSession, baseTablePath: String, level: String,
    toDate: DateTime, fromDate: DateTime = new DateTime("2016-01-01")): DataFrame = {

    import spark.implicits._

    // Parse Dates
    val toDateStr = toDate.toString(ArgsUtils.formatter)
    val fromDateStr = fromDate.toString(ArgsUtils.formatter)

    spark.read.parquet(baseTablePath)
      .filter($"dt".between(fromDateStr, toDateStr))
  }

}