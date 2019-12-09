package com.paytm.map.features.sanity

import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.Constants.{gaBackFillDays, gaDelayOffset}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.datasets.ReadBaseTable._
import org.apache.spark.sql.SparkSession
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.sanity.myUDFFunctionsX
import com.paytm.map.features.sanity.ParseSanityFeatConfig
import com.paytm.map.features.sanity.SanityConstants._
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import com.paytm.map.features.datasets.ReadBaseTable.defaultMinDate
import com.paytm.map.features.utils.FileUtils._
import org.apache.spark.sql.types.{DoubleType, StringType}

object MissingDatasetAlert extends MissingDatasetAlertJob with SparkJob with SparkJobBootstrap

trait MissingDatasetAlertJob {
  this: SparkJob =>

  val JobName = "MissingDatasetAlert"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)

    val baseTableBasePath = s"${settings.featuresDfs.rawTable}aggregates"
    val baseTableBasePathProd = s"${baseTableBasePath}".replace("/stg/", "/prod/")

    //level Identifiers for various level categories
    val levelAggIdf = Seq("L1", "L2", s"L3$l2RU", s"L3$l2BK", s"L3${l2EC}1", s"L3${l2EC}2", s"L3${l2EC}3", s"L3${l2EC}4")
    val walletAggIdf = Seq("PaymentFeatures", "WalletFeatures")
    val bankAggIdf = Seq("BankFeatures")
    val gaLevelAggIdf = Seq("L1", "L2", s"L2$l2RU", s"L3$l2RU", s"L3$l2BK", s"L3${l2EC}1", s"L3${l2EC}2", s"L3${l2EC}3", s"L3${l2EC}4", l2BK)

    // Paths for various level categories (base, wallet, bank and GA)
    val baseLevelPaths = levelAggIdf.map(levelIdf => s"$baseTableBasePathProd/" + levelIdf)
    val walletLevelPaths = walletAggIdf.map(levelIdf => s"$baseTableBasePathProd/" + levelIdf)
    val bankLevelPaths = bankAggIdf.map(levelIdf => s"$baseTableBasePathProd/" + levelIdf)
    val gaLevelPaths = gaLevelAggIdf.map(levelIdf => s"$baseTableBasePathProd/GAFeatures/" + levelIdf)

    // How many lookback days of data is required for each level
    val lookBackBaseLevel = 800
    val lookBackWalletLevel = 90
    val lookBackBankLevel = 90
    val lookBackGALevel = 90

    implicit def ord: Ordering[DateTime] = Ordering.by(_.getMillis)

    def getNotAvlDates(sparkSn: SparkSession, levelPath: String, endDate: DateTime, lookBackDaysLevel: Int): (String, Int, String, String, String, Int) = {

      val avlDates = getAvlTableDates(levelPath, sparkSn, "dt=") //Find Avl dates from given path
      val reqStartDateLevel = Seq(defaultMinDate, endDate.minusDays(lookBackDaysLevel - 1)).sorted(ord).last // Max date out of defaultMinDate or lookBack days ago for that level
      val datesLevelReq = dayIterator(reqStartDateLevel, endDate, ArgsUtils.formatter) //these dates are required

      // filter out dates which are not available in datesLevelReq
      val notAvlDates = datesLevelReq.map(reqDate => (reqDate, avlDates.contains(reqDate))).filter(!_._2)

      // returning, levelPath, number of missing dates, sample dates and other parameters
      (levelPath, notAvlDates.length, notAvlDates.take(10).map(_._1).mkString("|"),
        reqStartDateLevel.toString(ArgsUtils.formatter),
        endDate.toString(ArgsUtils.formatter),
        lookBackDaysLevel)
    }

    // Finding missing dates for individual level categories
    val baseLevelMissing = baseLevelPaths.map(lvlPath => getNotAvlDates(spark, lvlPath, targetDate, lookBackBaseLevel))
    val walletLevelMissing = walletLevelPaths.map(lvlPath => getNotAvlDates(spark, lvlPath, targetDate, lookBackWalletLevel))
    val bankLevelMissing = bankLevelPaths.map(lvlPath => getNotAvlDates(spark, lvlPath, targetDate, lookBackBankLevel))
    val gaLevelMissing = gaLevelPaths.map(lvlPath => getNotAvlDates(spark, lvlPath, targetDateGA, lookBackGALevel))

    // Creating report dataframe
    val missingDatesDf = Seq(
      baseLevelMissing,
      walletLevelMissing,
      bankLevelMissing,
      gaLevelMissing
    ).map(lvlRdd => lvlRdd.toDF(
      "Path",
      "MissingDatesCount",
      "SampleMissingDates",
      "StartDate",
      "EndDate",
      "LookBackDays"
    )
      .withColumn("Status", when($"MissingDatesCount" === 0, "Healthy").otherwise("Sick"))
      .withColumn("MissingPercent", $"MissingDatesCount".cast(DoubleType) / $"LookBackDays".cast(DoubleType))).reduce(_ union _)

    val missingDatesReport = missingDatesDf.select(
      $"Path",
      $"Status",
      $"MissingDatesCount",
      concat(round($"MissingPercent" * 100, 2).cast(StringType), lit(s"%")) as "MissingPercent",
      $"SampleMissingDates",
      $"StartDate",
      $"EndDate",
      $"LookBackDays"
    )

    //missingDatesReport.show(100, false)

    val measurementBasePath = s"${settings.featuresDfs.measurementPath}"

    val rawDatasetHealthReportPath = measurementBasePath + s"raw_dataset_health_reports/dt=${targetDateStr}"

    missingDatesReport.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(rawDatasetHealthReportPath)

    println("Report Written at " + rawDatasetHealthReportPath)

  }
}