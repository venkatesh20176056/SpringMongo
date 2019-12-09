package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.DataTables._

object CopyUpiFeatures extends CopyUpiFeaturesJob
  with SparkJob with SparkJobBootstrap

trait CopyUpiFeaturesJob {
  this: SparkJob =>

  val JobName = "BankFeatures"
  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)
    import spark.implicits._

    val aggUpiPath = s"${settings.featuresDfs.baseDFS.aggPath}/UPI"

    val bankFeatures: DataFrame =
      spark.read.parquet(settings.bankDfs.bankUpiPath)
        .where($"dt".between(startDateStr, targetDateStr))

    bankFeatures.moveHDFSData(dtSeq, aggUpiPath)
  }

}
