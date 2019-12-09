package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.ConvenientFrame.configureSparkForMumbaiS3Access
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AggFeaturesBills extends AggFeaturesBillsJob
  with SparkJob with SparkJobBootstrap

trait AggFeaturesBillsJob {
  this: SparkJob =>

  val JobName = "AggFeaturesBills"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    // Get the command line parameters
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.FormatPattern)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggBillsPath = s"${baseDFS.aggPath}/Bills/dt=$targetDateStr/"
    println(baseDFS.aggPath)

    val billsData = DataTables.billDueDateTable(spark, settings.datalakeDfs.billsPaths, settings.datalakeDfs.prepaidExpiry)

    val salesAggregates = billsData
      .addRULevel()
      .drop("operator")
      .addBillsAggregates(groupByCol = Seq("customer_id"), pivotCol = "RU_Level")

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    salesAggregates
      .addPrefixSuffixToColumns(prefix = "RU_", suffix = "_operator_due_dates", excludedColumns = Seq("customer_id"))
      .write.mode(SaveMode.Overwrite)
      .parquet(aggBillsPath)
  }

  private implicit class L3RUImplicits(dF: DataFrame) {
    def addRULevel(): DataFrame = {
      import dF.sparkSession.implicits._
      dF.withColumn(
        "RU_level",
        when($"service".like("%mobile%") && lower($"paytype").like("%prepaid%"), lit("MB_prepaid"))
          .when($"service".like("%mobile%") && lower($"paytype").like("%postpaid%"), lit("MB_postpaid"))
          .when($"service".like("%dth%"), lit("DTH"))
          .when($"service".like("%broadband%"), lit("BB"))
          .when($"service".like("%datacard%"), lit("DC"))
          .when($"service".like("%electricity%"), lit("EC"))
          .when($"service".like("%water%"), lit("WAT"))
          .when($"service".like("%gas%"), lit("GAS"))
          .when($"service".like("%landline%"), lit("LDL"))
          .when($"service".like("%financial services%"), lit("INS"))
          .when($"service".like("%loan%"), lit("LOAN"))
          .when($"service".like("%metro%") && not(lower($"operator").like("%delhi metro%")), lit("MTC"))
          .when($"service".like("%metro%") && lower($"operator").like("%delhi metro%"), lit("MT"))
          .when($"service".like("%devotion%"), lit("DV"))
          .when($"service".like("%google play%"), lit("GP"))
          .when($"service".like("%challan%"), lit("CHL"))
          .when($"service".like("%municipal payments%"), lit("MNC"))
          .when($"service".like("%toll tag%"), lit("TOLL"))
          .otherwise(lit(null))
      )
        .where($"RU_Level".isNotNull)
    }
  }

}