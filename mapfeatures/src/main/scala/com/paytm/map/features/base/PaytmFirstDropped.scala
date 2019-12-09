package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.utils.ConvenientFrame._

import com.paytm.map.features.utils.UDFs.readTable

object PaytmFirstDropped extends PaytmFirstDroppedJob
  with SparkJob with SparkJobBootstrap

trait PaytmFirstDroppedJob {
  this: SparkJob =>

  val JobName = "PaytmFirstDropped"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val lookBackDays: Int = args(1).toInt
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    import spark.implicits._

    val aggPaytmFirstPath = s"${settings.featuresDfs.baseDFS.aggPath}/PaytmFirstDropped"

    //Read Data
    val alipayPGOlap = readTable(spark, settings.datalakeDfs.alipayPgOlap, "dwh", "alipay_pg_olap",
      targetDateStr, startDateStr, false, partnCol = "ingest_date")

    // Get customers who dropped out at payment gateway stage from paytm first
    val paytmFirstDropped = alipayPGOlap
      .filter($"merchant" === "Paytm First" && $"responsecode".isin(227, 810)) // https://jira.mypaytm.com/browse/MAP-2528
      .select(
        $"cust_id" as "customer_id",
        $"txn_started_at".cast("date") as "dt",
        lit(1) as "paytm_first_dropped"
      ).distinct

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    paytmFirstDropped
      .coalescePartitions("dt", "customer_id", dtSeq)
      .write.partitionBy("dt")
      .mode(SaveMode.Overwrite)
      .parquet(aggPaytmFirstPath)
  }
}