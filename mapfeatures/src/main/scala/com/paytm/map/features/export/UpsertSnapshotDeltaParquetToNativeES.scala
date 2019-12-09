package com.paytm.map.features.export

import java.net.InetSocketAddress

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobInvalid, SparkJobValidation}
import org.apache.spark.sql._
import com.paytm.map.features.utils.{ArgsUtils, DataframeDelta, Settings}
import com.paytm.map.features.export.elasticsearch._
import org.joda.time.DateTime

/* Spark Submit:

  /bin/spark-submit --class com.paytm.shinra.export.UpsertSnapshotDeltaParquetToNativeES \
    --name "UpsertParquetToNativeES" \
    --master "yarn-cluster" \
    --queue "Midgar" \
    --driver-memory 4g \
    --num-executors 10 \
    --executor-memory 4g \
    --executor-cores 1 \
    shinra_1.0.0.jar \
    2016-02-12 127.0.0.1:9300 Midgar recommendation product key 10000 25 1 10

 */
object UpsertSnapshotDeltaParquetToNativeES extends UpsertSnapshotDeltaParquetToNativeESJob with SparkJob with SparkJobBootstrap {
}

trait UpsertSnapshotDeltaParquetToNativeESJob {
  this: SparkJob =>

  val JobName = "UpsertSnapshotDeltaParquetToNativeES"

  val ArgumentLength = 10

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    /** Preconditions **/
    if (args.length < ArgumentLength) {
      SparkJobInvalid("Must supply arguments, 'nodes', 'clusterName', 'index', 'type', 'id', 'batchSize', 'batchBytes', 'concurrentRequests', and 'flushTimeoutInSeconds'")
    } else {
      DailyJobValidation.validate(spark, args)
    }
  }

  def parseHosts(hosts: String): Seq[InetSocketAddress] = {
    hosts.split(",")
      .map {
        host =>
          {
            val Array(esHost, esPort) = host.split(":")
            new InetSocketAddress(esHost, esPort.toInt)
          }
      }
  }

  def parseDocumentKey(key: String): Option[String] = key match {
    case "None" => None
    case k      => Some(k)
  }

  def replaceDateTemplate(templatedIndex: String, date: DateTime): String = {
    def zeroPad(s: String): String = {
      if (s.length == 1) s"0$s"
      else s
    }

    templatedIndex.replaceAll("%Y", zeroPad(date.year.getAsString))
      .replaceAll("%m", zeroPad(date.monthOfYear.getAsString))
      .replaceAll("%d", zeroPad(date.dayOfMonth.getAsString))
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    val sparkContext = spark.sparkContext

    val ARG_NODES = 1
    val ARG_CLUSTER_NAME = 2
    val ARG_INDEX = 3
    val ARG_TYPE = 4
    val ARG_ID = 5
    val ARG_BATCH_SIZE = 6
    val ARG_BATCH_BYTES = 7
    val ARG_CONCURRENT_REQUESTS = 8
    val ARG_FLUSH_TIMEOUT_IN_SECONDS = 9
    val ARG_PATH_TO_PARQUET = 10

    val esHosts = parseHosts(args(ARG_NODES))
    val esClusterName = args(ARG_CLUSTER_NAME)
    val esIndex = args(ARG_INDEX)
    val esType = args(ARG_TYPE)
    val esId = parseDocumentKey(args(ARG_ID))
    val esBatchSize = args(ARG_BATCH_SIZE).toInt
    val esBatchBytes = args(ARG_BATCH_BYTES).toInt
    val esConcurrentRequests = args(ARG_CONCURRENT_REQUESTS).toInt
    val esFlushTimeoutInSeconds = args(ARG_FLUSH_TIMEOUT_IN_SECONDS).toLong

    val settings = new Settings
    import settings._

    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val targetDate = ArgsUtils.getTargetDate(args)
    val esPathToParquet = {
      if (args.length > ARG_PATH_TO_PARQUET)
        replaceDateTemplate(args(ARG_PATH_TO_PARQUET), targetDate)
      else
        featuresDfs.exportTable + "snapshot_delta/dt=" + targetDateStr
    }

    log.info {
      s"""UpsertSnapshotDeltaParquetToNativeES
          | |Configurations:
          | - 'node':                 ${esHosts.mkString(",")}
          | - 'clusterName':          $esClusterName
          | - 'index':                $esIndex
          | - 'type':                 $esType
          | - 'id':                   $esId
          | - 'batchSize':            $esBatchSize
          | - 'batchBytes':           $esBatchBytes
          | - 'concurrentRequests':   $esConcurrentRequests
          | - 'flushTimeoutInMillis': $esFlushTimeoutInSeconds
          | - 'pathToParquet':        $esPathToParquet
          |
      |Spark:
          | - appName:       ${sparkContext.appName}
          | - applicationId: ${sparkContext.applicationId}
          | - version:       ${sparkContext.version}
          | - master:        ${sparkContext.master}
          | - user:          ${sparkContext.sparkUser}
          | - startTime:     ${sparkContext.startTime}
     """.stripMargin
    }

    val parquetData = spark.read.parquet(esPathToParquet)

    val esClientConf = EsNativeTransportClientConf(
      transportAddresses = esHosts,
      transportSettings  = Map(
        EsNativeTransportClientConf.CONFIG_CLUSTER_NAME -> esClusterName
      )
    )

    val esMappingConf = EsDataFrameMappingConf(
      esMappingId = esId
    )

    val esWriteConf = EsDataFrameWriteConf(
      bulkActions           = esBatchSize,
      bulkSizeInMB          = esBatchBytes,
      concurrentRequests    = esConcurrentRequests,
      flushTimeoutInSeconds = esFlushTimeoutInSeconds
    )

    parquetData.nativeUpsertToEs(
      esIndex          = esIndex,
      esType           = esType,
      changeColumnName = DataframeDelta.ChangedFieldName,
      clientConf       = esClientConf,
      mappingConf      = esMappingConf,
      writeConf        = esWriteConf
    )
  }

}