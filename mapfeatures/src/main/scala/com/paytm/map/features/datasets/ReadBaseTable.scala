package com.paytm.map.features.datasets

import com.paytm.map.features.utils.ArgsUtils
import com.paytm.map.features.datasets.Constants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import com.paytm.map.features.utils.Settings

object ReadBaseTable {

  val defaultMinDate: DateTime = new DateTime("2016-01-01")
  def levelNameToID(levelPrefix: String): String = levelPrefix match {
    case `l1`          => "L1"
    case `l2RU`        => "L2"
    case `l2EC`        => "L2"
    case `l2BK`        => "L2"
    case `gaBK`        => "GAFeatures/BK"
    case `gaL3RU`      => "GAFeatures/L3RU"
    case `gaL3EC1`     => "GAFeatures/L3EC1"
    case `gaL3EC2`     => "GAFeatures/L3EC2"
    case `gaL3EC3`     => "GAFeatures/L3EC3"
    case "GAL3EC4_I"   => "GAFeatures/L3EC4"
    case "GAL3EC4_II"  => "GAFeatures/L3EC4"
    case "GAL3EC4_III" => "GAFeatures/L3EC4"
    case `gaL3BK`      => "GAFeatures/L3BK"
    case `gaL2RU`      => "GAFeatures/L2RU"
    case `gaL2EC`      => "GAFeatures/L2"
    case `gaL2BK`      => "GAFeatures/L2"
    case `gaL1`        => "GAFeatures/L1"
    case `payments`    => "PaymentFeatures"
    case `online`      => "OnlineFeatures"
    case `wallet`      => "WalletFeatures"
    case `bank`        => "BankFeatures"
    case other         => other
  }

  def readBaseTable(
    spark: SparkSession,
    baseTableBasePath: String,
    levelPrefix: String,
    toDate: DateTime,
    fromDate: DateTime = defaultMinDate
  ): DataFrame = {

    val toDateStr = toDate.toString(ArgsUtils.formatter)
    val fromDateStr = fromDate.toString(ArgsUtils.formatter)
    val levelID = levelNameToID(levelPrefix)

    val levelNameToIDCorrected = if (levelID.contains("_")) { levelID.split("_").head } else levelID

    val baseTablePath = s"$baseTableBasePath/$levelNameToIDCorrected/"
    // TODO V2: Collect all s3 paths as list and read just those dates instead of reading all and then filtering
    val baseData = spark.read.parquet(baseTablePath).filter(col("dt").between(fromDateStr, toDateStr))

    val renamedCols = baseData.columns.map { x =>
      levelID match {
        case "L3RU"             => col(x) as x.replace("L3_", "")
        case "L3BK"             => col(x) as x.replace("L3_", "")
        case "L3EC1"            => col(x) as x.replace("L3_EC1_", "EC1_")
        case "L3EC2_I"          => col(x) as x.replace("L3_EC2_", "EC2_")
        case "L3EC2_II"         => col(x) as x.replace("L3_EC2_", "EC2_")
        case "L3EC3"            => col(x) as x.replace("L3_EC3_", "EC3_")
        case "L3EC4_I"          => col(x) as x.replace("L3_EC4_", "EC4_")
        case "L3EC4_II"         => col(x) as x.replace("L3_EC4_", "EC4_")
        case "L3EC4_III"        => col(x) as x.replace("L3_EC4_", "EC4_")
        case "L2"               => col(x) as x.replace("L2_", "")
        case "L1"               => col(x) as x.replace("L1_", s"${l1}_")
        case "GAFeatures/L3EC1" => col(x) as x.replace("L3GA_EC1_", "EC1_")
        case "GAFeatures/L3EC2" => col(x) as x.replace("L3GA_EC2_", "EC2_")
        case "GAFeatures/L3EC3" => col(x) as x.replace("L3GA_EC3_", "EC3_")
        case "GAFeatures/L3EC4" => col(x) as x.replace("L3GA_EC4_", "EC4_")
        case "GAFeatures/L3BK"  => col(x) as x.replace("L3GA_BK_", "BK_")
        case "GAFeatures/L2"    => col(x) as x.replace("L2GA_", "")
        case "GAFeatures/L2RU"  => col(x) as x.replace("L2GA_", "")
        case "GAFeatures/L1"    => col(x) as x.replace("L1GA_", s"${l1}_")
        case _                  => col(x)
      }
    }

    val renamedBase = baseData.select(renamedCols: _*)

    // Just select columns relevant for the vertical. Useful for L2.
    //TODO V2: Add day_of_week in base table
    val extraCols = Seq("customer_id", "dt").map(col)
    val levelPrefixCorrected = if (levelPrefix.startsWith("GAL3") || levelPrefix.startsWith("GAL2")) levelPrefix.substring(4) else if (levelPrefix.startsWith("L3") || levelPrefix.startsWith("GA")) levelPrefix.substring(2) else levelPrefix

    val levelPrefixFurtherCorrected = if (levelPrefixCorrected.contains("_")) { levelPrefixCorrected.split("_").head } else levelPrefixCorrected
    val levelCols = renamedBase.columns.filter(_.contains(levelPrefixFurtherCorrected + "_")).map(col)

    renamedBase.select(extraCols ++ levelCols: _*)
      .withColumn("day_of_week", from_unixtime(unix_timestamp(col("dt"), "yyyy-MM-dd"), "E"))
  }

  def filterBaseTableTimeSeries(
    baseTable: DataFrame,
    toDate: DateTime
  ): Seq[(DataFrame, Int)] = {

    val durations = timeSeriesDurations
    val fromDates = durations.map(toDate.minusDays)
    val toDateStr = toDate.toString(ArgsUtils.formatter)

    val timeSeriesDFs = for (fromDate <- fromDates) yield {
      val fromDateStr = fromDate.toString(ArgsUtils.formatter)
      val toDateStr = toDate.toString(ArgsUtils.formatter)
      // If job is run on say 11th Jun, toDate would be 10thJune
      // 10thJune minus say 3 days would be 7th June
      // So we should read 8th, 9th and 10th June
      // Therefore, > fromDateStr and <= toDate
      baseTable.filter(col("dt") > fromDateStr && col("dt") <= toDateStr)
    }
    timeSeriesDFs.zip(durations)
  }

  def filterBaseTableMonthlyTimeSeries(
    baseTable: DataFrame,
    toDate: DateTime
  ): Seq[(DataFrame, Int)] = {
    monthlyAggDurations.map(lookBackMonth => {
      val prevMonth = toDate.minusMonths(lookBackMonth)
      val fromDate = prevMonth.toString(ArgsUtils.formatter).slice(0, 8) + "01"
      val maxDate = prevMonth.plusMonths(1).toString(ArgsUtils.formatter).slice(0, 8) + "01"
      val toDateStr = toDate.toString(ArgsUtils.formatter)
      (baseTable.filter(col("dt") >= fromDate && col("dt") < maxDate && col("dt") <= toDateStr), lookBackMonth)
    })

  }

}