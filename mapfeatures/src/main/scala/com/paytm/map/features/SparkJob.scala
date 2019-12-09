package com.paytm.map.features

import org.apache.spark.internal.Logging
import com.paytm.map.features.Metrics.{MetricsDataFrame, MetricsInput, MetricsLevelOutputPath}
import com.paytm.map.features.utils.DataframeMetrics.{ColumnStatistics, TableStatistics}
import com.paytm.map.features.utils.monitoring.Monitoring
import com.paytm.map.features.utils.{ArgsUtils, DataframeMetrics, Settings}
import org.apache.spark.sql.{SaveMode, SparkSession}

trait SparkJobBase extends Logging with Serializable {
  type C

  val JobName: String

  def runJob(sc: C, settings: Settings, args: Array[String]): Unit

  def validate(sc: C, settings: Settings, args: Array[String]): SparkJobValidation

}

trait SparkJob extends SparkJobBase {
  type C = SparkSession
}

trait SparkMetricJob extends SparkJob with Monitoring {
  def metricsInputs(sc: C, settings: Settings, args: Array[String]): Seq[MetricsInput]

  def publishMetrics(sc: C, paths: Seq[MetricsLevelOutputPath]): Unit = {
    val metrics = paths.map {
      case p =>
        MetricsDataFrame(
          p.label,
          sc.read.parquet(p.tableMetricsPath),
          sc.read.parquet(p.columnMetricsPath)
        )
    }
    publishMetrics(metrics)
  }

  def publishMetrics(metrics: Seq[MetricsDataFrame]) = {
    metrics.foreach {
      dfMetrics =>
        import dfMetrics.tableMetrics.sparkSession.implicits._
        import DataframeMetrics.TypeTagCast

        TableStatistics.metrics.foreach { metric =>

          val value = dfMetrics.tableMetrics.where($"summary" <=> metric.name)
            .select("table_metrics").head().getAs[String](0)
          monitor.gauge(s"${dfMetrics.label}.table_metrics.${metric.name}", () => {
            value.fromTypeTag(metric.dataType)
          })
        }

        val metricColumns = dfMetrics.columnMetrics.columns.filter(_ != "summary")
        ColumnStatistics.metrics.foreach {
          metric =>
            metricColumns.foreach {
              col =>
                val value = dfMetrics.columnMetrics.where($"summary" <=> metric.name)
                  .select(col).head.getAs[String](0)
                monitor.gauge(s"${dfMetrics.label}.column_metrics.$col.${metric.name}", () => {
                  value.fromTypeTag(metric.dataType)
                })
            }
        }
    }
  }

  def metricRootOutputPath(settings: Settings, args: Array[String]): String = {
    val targetDateStr = ArgsUtils.getTargetDateStr(args)
    s"${settings.featuresDfs.metricTable}/dt=${targetDateStr}"
  }

  def metricsOutputPaths(rootPath: String, label: String): MetricsLevelOutputPath = {
    MetricsLevelOutputPath(label, s"$rootPath/label=${label}/metrics=table", s"$rootPath/label=${label}/metrics=column")
  }

  def generateMetrics(sc: C, metricRootPath: String, inputs: Seq[MetricsInput]): Seq[MetricsLevelOutputPath] = {
    inputs.map { input =>
      val df = sc.read.parquet(input.inputPath)
      val metricsPath = metricsOutputPaths(metricRootPath, input.label)
      DataframeMetrics.getTableMetrics(df).write.mode(SaveMode.Overwrite).parquet(metricsPath.tableMetricsPath)
      DataframeMetrics.getColumnMetrics(df, input.columns).write.mode(SaveMode.Overwrite).parquet(metricsPath.columnMetricsPath)
      metricsPath
    }
  }
}
