package com.paytm.map.features.Chakki

import com.paytm.map.features.Chakki.FeatChakki.Constants._
import com.paytm.map.features.Chakki.FeatChakki.{FeatExecutor, ResolveFeatures}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.ArgsUtils._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import scala.util.Try

object CreateDataSet extends CreateDataSetJob with SparkJob with SparkJobBootstrap

trait CreateDataSetJob {
  this: SparkJob =>

  val JobName = "CreateDataSet"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val levelPrefix = args(1)
    val groupPrefix = args(2)
    val cacheTo = Try(args(3).toLowerCase).getOrElse("memory") //s3, memory, disk, nocaching
    val mergeBaseTableSchema = Try(args(4).toBoolean).getOrElse(false)

    // Parse the multiple execution stage if any
    val (execLevels, level, writePathSuffix) = getLevels(levelPrefix)
    println(execLevels, level, writePathSuffix)

    // Read Paths
    val baseTableBasePath = settings.featuresDfs.baseDFS.aggPath
    val baseTablePath = s"$baseTableBasePath/${level2BaseTable(level)}"
    val customerSegmentPath = featuresDfs.featuresTable + groupPrefix + s"/ids/dt=$targetDateStr"
    val baseTableCachePath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}/cache/$writePathSuffix"
    val writePath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}/$writePathSuffix/dt=$targetDateStr"

    // Read Specs
    val SpecSheetPath = s"${settings.featuresDfs.resources}${featuresDfs.specSheets}"
    val FeatListPath = s"${settings.featuresDfs.resources}${featuresDfs.featureList}"

    println(baseTablePath, customerSegmentPath, writePath)

    // Resolve Features
    val resolvedFeatures = execLevels.map { execLevel =>
      val resolvedFeats = ResolveFeatures(spark, FeatListPath, SpecSheetPath, execLevel)
      (execLevel, resolvedFeats)
    }.toMap

    //Data Read Date Offseted for GA
    val (readDate, fromDate) = getDates(level, targetDate,
      getLatestTableDate(baseTablePath, spark, targetDate.plusDays(1).toString(formatter), "dt=").orNull)
    println(readDate.toString(), fromDate.toString())

    // Read datasets
    val customerSegment = spark.read.parquet(customerSegmentPath)

    // This is to optimize the join and caching with customer_segment
    val baseFeatures = resolvedFeatures
      .flatMap { case (_, resolvedFeats) => resolvedFeats.baseFeaturesRequired ++ Set("customer_id", "dt") }
      .toSeq.distinct

    // Read baseTable
    val baseTable = readBaseTable(spark, baseTablePath, level, readDate, fromDate, mergeSchema = mergeBaseTableSchema)
      .select(baseFeatures.head, baseFeatures.tail: _*)
      .join(customerSegment, Seq("customer_id"), "left_semi")

    val baseTableCached = cacheTo match {
      case "s3" =>
        // persist as parquet to s3 for dataset too large to fit in memory, also to leverage parquet pushdown filters
        baseTable
          .write
          .mode(SaveMode.Overwrite)
          .parquet(baseTableCachePath)

        spark.read.parquet(baseTableCachePath)
      case "memory" =>
        baseTable.persist(StorageLevel.MEMORY_AND_DISK_SER)
      case "disk" =>
        baseTable.persist(StorageLevel.DISK_ONLY)
      case "nocaching" =>
        baseTable
    }

    val requestedFeat = resolvedFeatures.flatMap(_._2.featureSet).toSet

    if (requestedFeat.nonEmpty) {
      // Execute Features.
      val featData = getTransformedData(resolvedFeatures, baseTableCached, readDate)

      // Write the output
      featData
        .write
        .mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }

  /**
   * Offset wherever require the startdate and adjust the read Start date based on the logic.
   * @param level : DataLevel to be read.
   * @param targetDate : targetDate passed as argument.
   * @return : Start and endDate for data reading.
   */
  def getDates(level: String, targetDate: DateTime, avlDate: String): (DateTime, DateTime) = {

    //TODO : Check With Prabhat , the logic
    val gaDate = formatter.parseDateTime(Seq(targetDate.minusDays(2).toString(FormatPattern), avlDate).filter(_ != null).min)
    val readDate = if (isGA(level)) gaDate else targetDate
    val fromDate = {
      val lookBackDays = level2LookBack(level)
      if (lookBackDays.nonEmpty) readDate.minusDays(lookBackDays.get)
      else new DateTime("2016-01-01")
    }
    (readDate, fromDate)
  }

  /**
   * Parse the Level specific Information from the level prefix
   * @param levelPrefix :  LevelPrefix parameter parsed from the arguments
   * @return : Tuple of Execlevels, Level and writePath Suffix.
   */
  def getLevels(levelPrefix: String): (Seq[String], String, String) = {

    // Parse the multiple execution stage if any
    val execLevels = levelPrefix.split(",")

    val level = {
      val levels = execLevels.map(execLevel => execLevel2Level(execLevel)).distinct
      //If the execution Levels has different source data then raise alert
      assert(levels.length == 1)
      levels.head
    }

    val writePathSuffix = {
      val writePaths = execLevels.map(execLevel => execLevel2WritePath(execLevel)).distinct
      //IF execlevels passed here has multiple output path then USE Azkaban for the functionality.
      // Functionality here is to only partition execution stage for failure safe smaller stages.
      assert(writePaths.length == 1)
      writePaths.head
    }

    (execLevels, level, writePathSuffix)
  }

  /**
   * Function reads the base/aggregates data for given execlevels, from an pre-decided date to current/execution date.
   *
   * @param spark             : SparkSession Object
   * @param baseTablePath : Aggregates / base Table path
   * @param fromDate          : TargetDate or the end date to read data of
   * @param toDate            : FromDate or the start date to read data from
   * @return : Dataframe of base table specified by exec-levels filtered on from-to dates
   */
  def readBaseTable(spark: SparkSession, baseTablePath: String, level: String,
    toDate: DateTime, fromDate: DateTime = new DateTime("2016-01-01"), mergeSchema: Boolean): DataFrame = {

    import spark.implicits._

    // Parse Dates
    val toDateStr = toDate.toString(ArgsUtils.formatter)
    val fromDateStr = fromDate.toString(ArgsUtils.formatter)

    val readData = spark.read.option("mergeSchema", mergeSchema).parquet(baseTablePath)
    (if (level == "LMS" || level == "LMSV2")
      readData.withColumn("dt", lit(toDateStr))
    else readData)
      .filter($"dt".between(fromDateStr, toDateStr))
  }

  /**
   * Function Chooses between multi stage execution vs single stage execution to avoid memory caching
   *
   * @param resolvedFeatures : Resolved Feature set for each execution level
   * @param baseTable        : data on which feature set has to be transformed
   * @param readDate         : targetDate for the base time series calculation.
   * @return : Transformed data with feature specs applied.
   */
  def getTransformedData(resolvedFeatures: Map[String, ResolveFeatures], baseTable: DataFrame, readDate: DateTime): DataFrame = {

    if (resolvedFeatures.size > 1) {
      // If Multi Stage Execution
      resolvedFeatures.map {
        case (_, resolvedFeats) =>
          //Apply Features
          FeatExecutor(resolvedFeats.featureSet, baseTable, readDate)
            .execute()
      }.toSeq.joinAllWithoutRepartiton(Seq("customer_id"), "outer")
    } else {
      // Apply Features
      val (_, resolvedFeats) = resolvedFeatures.head
      FeatExecutor(resolvedFeats.featureSet, baseTable, readDate).execute()
    }

  }
}