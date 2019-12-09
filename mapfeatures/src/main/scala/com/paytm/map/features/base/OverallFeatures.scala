package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime

object OverallFeatures extends OverallFeaturesJob
  with SparkJob with SparkJobBootstrap

trait OverallFeaturesJob {
  this: SparkJob =>

  val JobName = "OverallFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggL1Path = s"${baseDFS.aggPath}/OverallFeatures"

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    customerFactFeatures(settings, dtSeq)(spark)
      .renameColumns(prefix = "OVERALL_", excludedColumns = Seq("customer_id", "dt"))
      .coalescePartitions("dt", "customer_id", dtSeq)
      .cache()
      .moveHDFSData(dtSeq, aggL1Path)
  }

  def customerFactFeatures(settings: Settings, dtSeq: Seq[String])(implicit spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit

    val schema = StructType(Array(StructField("customer_id", LongType), StructField("dt", StringType)))
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val dfs = dtSeq
      .map { dt =>
        val datasetPath = settings.datalakeDfs.factCustomer.get(s"day_id=$dt")
        datasetPath match {
          case Some(path) => spark.read.parquet(path).select("customer_id").withColumn("dt", lit(dt))
          case None       => emptyDf
        }
      }.reduce(_ union _)

    dfs
      .createOrReplaceTempView("fact_customer")

    spark.sql(
      """
          |select
          |	customer_id,
          | dt,
          |	dt as first_transaction_date
          |from
          |	fact_customer
          |group by
          |	customer_id, dt
          |""".stripMargin
    )
  }

}