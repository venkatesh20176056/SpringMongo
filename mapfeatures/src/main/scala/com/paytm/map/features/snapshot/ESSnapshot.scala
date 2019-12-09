package com.paytm.map.features.snapshot

import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, DataframeDelta, FileUtils, Settings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException

object ESSnapshot extends ESSnapshotJob
  with SparkJob with SparkJobBootstrap

trait ESSnapshotJob {
  this: SparkJob =>

  val JobName = "ESSnapshot"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings._
    import spark.implicits._

    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    val Array(host, port, _) = args(1).split(":")
    val indexName = args(2)
    val indexType = args(3)
    val numBuckets = args(4).toInt

    val options = Map(
      "pushdown" -> "true",
      "es.nodes" -> host,
      "es.port" -> port,
      "es.read.metadata" -> "true",
      "es.read.metadata.field" -> "metadata",
      "es.nodes.wan.only" -> "true"
    )

    val outputPath = s"${featuresDfs.exportTable}/es_snapshot/$indexName/dt=${targetDateStr}"
    Option(new Path(outputPath)).foreach {
      path =>
        val fs = FileUtils.fs(path, spark)
        fs.delete(path, true)
    }

    for (i <- 1 to numBuckets) {
      try {
        spark.read.format("org.elasticsearch.spark.sql")
          .options(options)
          .load(s"${indexName}_$i/$indexType")
          .select($"metadata._id".cast(LongType) as "customer_id")
          .distinct
          .write.mode(SaveMode.Append).parquet(outputPath)
      } catch {
        case e: EsHadoopIllegalArgumentException => log.warn(s"no index is found for: ${indexName}_$i/$indexType")
      }
    }
  }
}
