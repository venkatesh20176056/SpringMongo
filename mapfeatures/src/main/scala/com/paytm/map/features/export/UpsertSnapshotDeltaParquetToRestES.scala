package com.paytm.map.features.export

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.export.elasticsearch._
import com.paytm.map.features.utils.{ArgsUtils, DataframeDelta, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobInvalid, SparkJobValidation}
import com.paytm.map.features.utils.UDFs.isAllNumeric
import org.apache.http.HttpHost
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime

object UpsertSnapshotDeltaParquetToRestES extends UpsertSnapshotDeltaParquetToRestESJob with SparkJob with SparkJobBootstrap {
}

trait UpsertSnapshotDeltaParquetToRestESJob {
  this: SparkJob =>

  val JobName = "UpsertSnapshotDeltaParquetToRestES"

  val ArgumentLength = 10

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def parseHosts(hosts: String): Seq[HttpHost] = {
    hosts.split(",")
      .map {
        host =>
          {
            val Array(esHost, esPort, protocol) = host.split(":")
            new HttpHost(esHost, esPort.toInt, protocol)
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
    import spark.implicits._

    val sparkContext = spark.sparkContext

    val ARG_NODES = 1
    val ARG_INDEX = 2
    val ARG_TYPE = 3
    val ARG_ID = 4
    val ARG_BATCH_SIZE = 5
    val ARG_BATCH_BYTES = 6
    val ARG_CONCURRENT_REQUESTS = 7
    val ARG_FLUSH_TIMEOUT_IN_SECONDS = 8
    val ARG_FEATURESET = 9
    val ARG_UPSERT = 10
    val ARG_NBUCKET = 11

    val esHosts = parseHosts(args(ARG_NODES))
    val esIndex = args(ARG_INDEX)
    val esType = args(ARG_TYPE)
    val esId = parseDocumentKey(args(ARG_ID))
    val esBatchSize = args(ARG_BATCH_SIZE).toInt
    val esBatchMBytes = args(ARG_BATCH_BYTES).toInt
    val esConcurrentRequests = args(ARG_CONCURRENT_REQUESTS).toInt
    val esFlushTimeoutInSeconds = args(ARG_FLUSH_TIMEOUT_IN_SECONDS).toLong
    val groupPrefix = args(ARG_FEATURESET)

    val settings = new Settings
    import settings._

    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val targetDate = ArgsUtils.getTargetDate(args)

    val esPathToParquet = s"${featuresDfs.featuresTable}$groupPrefix/${featuresDfs.joined}dt=" + targetDateStr

    val withUpsert = {
      if (args.length > ARG_UPSERT)
        args(ARG_UPSERT).toBoolean
      else
        false
    }

    val nBuckets = if (args.length > ARG_NBUCKET) {
      args(ARG_NBUCKET).toInt
    } else {
      settings.esExportCfg.numBuckets
    }

    val statsOutput = featuresDfs.exportTable + s"es_upload_stats/${groupPrefix}/dt=" + targetDateStr

    log.info {
      s"""UpsertSnapshotDeltaParquetToRestES
          |
      |Configurations:
          | - 'node':                 ${esHosts.mkString(",")}
          | - 'index':                $esIndex
          | - 'type':                 $esType
          | - 'id':                   $esId
          | - 'batchSize':            $esBatchSize
          | - 'batchMBytes':           $esBatchMBytes
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

    val esClientConf = EsRestClientConf(
      httpHosts  = esHosts,
      numBuckets = nBuckets
    )

    val esMappingConf = EsDataFrameMappingConf(
      esMappingId = esId
    )

    val esWriteConf = EsDataFrameWriteConf(
      bulkActions           = esBatchSize,
      bulkSizeInMB          = esBatchMBytes,
      concurrentRequests    = esConcurrentRequests,
      flushTimeoutInSeconds = esFlushTimeoutInSeconds,
      backoffInMillis       = settings.esExportCfg.backoffInMillis,
      numRetries            = settings.esExportCfg.numRetries

    )

    val nonExport: Array[String] = spark.read
      .text("s3a://midgar-aws-workspace/prod/mapfeatures/no_export")
      .collect
      .map(r => r.getString(0))

    parquetData
      .filter(isAllNumeric($"customer_id"))
      .drop(nonExport: _*)
      .restUpsertToEs(
        esIndex          = esIndex,
        esType           = esType,
        changeColumnName = DataframeDelta.ChangedFieldName,
        clientConf       = esClientConf,
        mappingConf      = esMappingConf,
        writeConf        = esWriteConf,
        withUpsert       = withUpsert
      )

    import spark.implicits._
    // export statistics about the update
    val totalDelta = parquetData.count
    val stats = Seq(("total_number_changes_exported", totalDelta)).toDF("summary", "stats")
    stats.write.mode(SaveMode.Overwrite).parquet(statsOutput)
  }
}