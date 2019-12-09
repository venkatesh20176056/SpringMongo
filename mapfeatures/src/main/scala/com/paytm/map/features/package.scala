package com.paytm.map

import scala.reflect.runtime.universe._
import org.apache.spark.sql.DataFrame

package object features {
  object Metrics {

    case class Metric[T](name: String, dataType: TypeTag[T])
    case class MetricsInput(label: String, inputPath: String, columns: Seq[String])
    case class MetricsIOPath(label: String, inputPath: String, outputPath: String)
    case class MetricsLevelOutputPath(label: String, tableMetricsPath: String, columnMetricsPath: String)
    case class MetricsDataFrame(label: String, tableMetrics: DataFrame, columnMetrics: DataFrame)

  }
}
