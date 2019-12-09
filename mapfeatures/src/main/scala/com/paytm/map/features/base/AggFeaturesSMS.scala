package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.ConvenientFrame.configureSparkForMumbaiS3Access
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{SaveMode, SparkSession}

object AggFeaturesSMS extends AggFeaturesSMSJob
  with SparkJob with SparkJobBootstrap

trait AggFeaturesSMSJob {
  this: SparkJob =>

  val JobName = "AggFeaturesSMS"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    // Get the command line parameters
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.FormatPattern)
    val minusOneDateStr = targetDate.minusDays(1).toString(ArgsUtils.FormatPattern)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val dlPaths = settings.datalakeDfs
    val aggSMSPath = s"${baseDFS.aggPath}/SMS/dt=$targetDateStr/"
    println(baseDFS.aggPath)

    // Read target day minus one's partition only
    val targetPurchasePath = dlPaths.smsPurchases.filter(_.contains(s"dl_last_updated=$minusOneDateStr"))
    val targetBillsPath = dlPaths.smsUserBills.filter(_.contains(s"dl_last_updated=$minusOneDateStr"))
    val targetTravelPath = dlPaths.smsTravel.filter(_.contains(s"dl_last_updated=$minusOneDateStr"))

    // Check if any of the table isn't available
    val anyTableEmpty = targetBillsPath.isEmpty || targetPurchasePath.isEmpty || targetTravelPath.isEmpty

    if (anyTableEmpty) {
      log.warn("Datalake does not have (target day - 1)'s partition!")
    } else {
      val smsData = DataTables.SMSTable(spark, targetPurchasePath, targetBillsPath, targetTravelPath)

      val smsAggregates = smsData
        .addSMSAggregates(groupByCol = Seq("customer_id"))

      // Final Aggregates
      configureSparkForMumbaiS3Access(spark)
      smsAggregates
        .addPrefixSuffixToColumns(prefix = "SMS_", excludedColumns = Seq("customer_id"))
        .write.mode(SaveMode.Overwrite)
        .parquet(aggSMSPath)
    }
  }
}