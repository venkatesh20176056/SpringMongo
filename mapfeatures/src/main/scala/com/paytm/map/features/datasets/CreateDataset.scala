package com.paytm.map.features.datasets

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.datasets.ReadBaseTable._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

object CreateDataset extends CreateDatasetJob with SparkJob with SparkJobBootstrap

trait CreateDatasetJob {
  this: SparkJob =>

  val JobName = "CreateDataset"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val levelPrefix = args(1)
    val groupPrefix = args(2)
    val outPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}/$levelPrefix"
    val baseTableBasePath = s"${settings.featuresDfs.rawTable}/aggregates"
    val customerSegmentPath = featuresDfs.featuresTable + groupPrefix + s"/ids/dt=$targetDateStr"

    // Constants
    val keyColumns = Constants.getKey
    val allAgg = Constants.getAllAgg(levelPrefix)
    val sortByFirst = Constants.getSortByCol(isFirst = true, levelPrefix)
    val sortByLast = Constants.getSortByCol(isFirst = false, levelPrefix)
    val selectFirstCol = Constants.getSelectCol(levelPrefix, isFirst = true)
    val selectLastCol = Constants.getSelectCol(levelPrefix, isFirst = false)
    val joinType = "left_outer"

    // Read datasets
    val customerSegment = spark.read.parquet(customerSegmentPath)

    val baseTable = readBaseTable(spark, baseTableBasePath, levelPrefix, targetDate).
      join(customerSegment, Seq("customer_id"), "left_semi").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    // todo: broadcast
    val baseTableTS = filterBaseTableTimeSeries(baseTable, targetDate)
    val baseTableMonthlyTS = filterBaseTableMonthlyTimeSeries(baseTable, targetDate)

    // WORK
    val sales = baseTable
      .groupByAggregate(
        keyColumns,
        allAgg
      ).repartition(500)

    val salesTS = baseTableTS.map {
      case (df, duration) =>
        (df.groupByAggregate(
          keyColumns,
          Constants.getTimeSeriesSalesAgg(levelPrefix, duration)
        ).repartition(500), duration)
    }.sortBy(-_._2).map(_._1).joinAll(keyColumns, "left_outer")

    val salesMonthly = baseTableMonthlyTS.map {
      case (df, duration) =>
        (df.groupByAggregate(
          keyColumns,
          Constants.getMonthlyTimeSeriesSalesAggAll(Seq(levelPrefix), duration)
        ).repartition(500), duration)
    }.sortBy(-_._2).map(_._1).joinAll(keyColumns, "left_outer")

    val first = baseTable
      .getFirstLast(
        keyColumns,
        sortByFirst,
        selectFirstCol,
        isFirst = true
      ).repartition(500)

    val last = baseTable
      .getFirstLast(
        keyColumns,
        sortByLast,
        selectLastCol,
        isFirst = false
      ).repartition(500)

    val features = sales
      .join(salesTS, keyColumns, joinType)
      .join(first, keyColumns, joinType)
      .join(last, keyColumns, joinType)
      .join(salesMonthly, keyColumns, joinType)

    features.saveParquet(s"$outPath/dt=$targetDateStr", 500)
  }
}