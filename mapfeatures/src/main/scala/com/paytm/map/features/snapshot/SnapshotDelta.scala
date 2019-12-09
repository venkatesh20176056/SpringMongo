package com.paytm.map.features.snapshot

import com.paytm.map.features.Metrics.MetricsInput
import com.paytm.map.features._
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.export.elasticsearch.EsRestDataFrameUpsertWriter.getCustomerIdIndex
import com.paytm.map.features.utils.{ArgsUtils, DataframeDelta, FileUtils, Settings}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object SnapshotDelta extends SnapshotDeltaJob
    with SparkMetricJob with SparkJobBootstrap {
  override def metricsInputs(sc: SparkSession, settings: Settings, args: Array[String]): Seq[MetricsInput] = {

    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val featureSetName = args(2)
    val deltaRootPath = s"${settings.featuresDfs.exportTable}/snapshot_delta/$featureSetName"
    val inputPath = s"${deltaRootPath}/dt=${targetDateStr}"
    Seq(MetricsInput(s"${JobName}_$featureSetName", inputPath, Seq("customer_id", DataframeDelta.ChangedFieldName)))
  }
}

trait SnapshotDeltaJob {
  this: SparkJob =>

  val JobName = "SnapshotDelta"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings._
    import spark.implicits._

    spark.sqlContext.sql("set spark.sql.shuffle.partitions=400")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val featureSetPath = args(1)
    val featureSetName = args(2)
    val indexName = args(3)
    val forceReupload = if (args.length == 5) args(4).toBoolean else false

    val deltaRootPath = s"${settings.featuresDfs.exportTable}/snapshot_delta/$featureSetName"
    val outputPath = s"${deltaRootPath}/dt=${targetDateStr}"
    val esSnapshotPath = s"${settings.featuresDfs.exportTable}/es_snapshot/$indexName"

    val esStatsPath = featuresDfs.exportTable + s"es_upload_stats/${featureSetName}"

    val lastSuccessfulESUploadDate = FileUtils.getLatestTableDate(esStatsPath, spark, targetDateStr, "dt=")
    log.warn(s"Last delta is generated on: $lastSuccessfulESUploadDate")

    val currentSnapshot = spark.read.parquet(s"${featureSetPath}/dt=${targetDateStr}").repartition(1000)
    val currentDelta =
      if (lastSuccessfulESUploadDate.isDefined && !forceReupload) {

        def reconcileES(prevSnapshot: DataFrame, esSnapshot: DataFrame) = {
          val rowDelta = DataframeDelta.RowDelta(esSnapshot, prevSnapshot, Seq("customer_id"))

          //reinsert those rows to ES by deleting it from the prevSnapshot to
          // ensure the currentSnapshot will take care of it
          prevSnapshot.as("t1")
            .join(
              rowDelta.insertedRows.select("customer_id").as("t2"),
              $"t1.customer_id" <=> $"t2.customer_id",
              "left_outer"
            )
            .where($"t2.customer_id".isNull)
            .drop($"t2.customer_id")
        }

        //reconcilation
        val reconciledSnapshot = {
          val esSnapshot = spark.read.parquet(s"${esSnapshotPath}/dt=${targetDateStr}")
          val prevSnapshot = spark.read.parquet(s"${featureSetPath}/dt=${lastSuccessfulESUploadDate.get}")
          log.warn(s"prev snapshot before: ${prevSnapshot.count}")
          reconcileES(prevSnapshot, esSnapshot)
        }
        log.warn(s"prev snapshot before: ${reconciledSnapshot.count}")

        DataframeDelta.computeUpsert(reconciledSnapshot, currentSnapshot, Seq("customer_id"))
      } else {
        val prevSnapshot = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], currentSnapshot.schema)
        DataframeDelta.computeUpsert(prevSnapshot, currentSnapshot, Seq("customer_id"))
      }

    val nBuckets = settings.esExportCfg.numBuckets
    val indexUDF = udf { customerId: Long => getCustomerIdIndex(nBuckets)(customerId.toString) }

    currentDelta.withColumn("index_number", indexUDF($"customer_id"))
      .write.mode(SaveMode.Overwrite).partitionBy("index_number").parquet(outputPath)
  }

  def bucketize(numBuckets: Int = 10)(customerId: String): Int = {
    import scala.util.hashing.MurmurHash3
    val hashedCustomerId = Math.abs(MurmurHash3.stringHash(customerId))
    hashedCustomerId % numBuckets
  }
}
