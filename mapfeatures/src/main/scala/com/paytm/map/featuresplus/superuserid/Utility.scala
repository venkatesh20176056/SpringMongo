package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.utils.ArgsUtils
import com.paytm.map.features.utils.ArgsUtils.formatter
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Days}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.rdd.RDD

object Utility {

  case class DateBound(date: DateTime, lookBack: Int) {
    val endDate: String = date.toString(ArgsUtils.FormatPattern)
    val startDate: String = date.minusDays(lookBack).toString(ArgsUtils.FormatPattern)
  }

  class FunctionsClass {
    def getLatestDate(path: String, date: String): String = {
      val fs = FileSystem.get(new java.net.URI(path), new Configuration())
      val file = fs.listStatus(new Path(path)).
        filter(elem => elem.getPath.toString.split("/dt=").last <= s"$date").
        map(elem => elem.getPath.toString.split("/dt=").last).max
      file
    }

    def checkFileExists(filePath: String)(implicit spark: SparkSession): Boolean = {
      val path = new Path(s"$filePath/_SUCCESS")
      path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)
    }

    def dateDifference(startDate: String, endDate: String): Int =
      Days.daysBetween(formatter.parseDateTime(startDate), formatter.parseDateTime(endDate)).getDays
  }
  val Functions = new FunctionsClass

  class DFFunctionsClass {
    def addToColumnName(columnsToRename: Seq[String], add: String): Seq[Column] = columnsToRename.map(r => col(r).as(s"$r$add"))

    def conditionPrimaryCol(arr: Seq[Column]): Seq[Column] = arr.map(r => r.isNotNull && (length(r) > 2))

    def rowSum(arr: Seq[Column]): Column = arr.reduce(_ + _)
  }
  val DFFunctions = new DFFunctionsClass

  class UDFClass {

    def minSimilarity: UserDefinedFunction = udf[Long, Long, Long]((a: Long, b: Long) => { if (a > b) b else a })

    def boolSimilarity: UserDefinedFunction = udf[Int, String, String]((a: String, b: String) =>
      if (Option(a).isDefined && Option(b).isDefined && (a == b)) 1 else 0)

    def GMSimilarity: UserDefinedFunction = udf[Double, Double, Double]((a: Double, b: Double) =>
      scala.math.log10(1 + scala.math.pow(a * b, 0.5)))

    def dateSimilarity: UserDefinedFunction = udf[Double, String, String] { (a: String, b: String) =>
      import org.joda.time.Days
      import org.joda.time.format.DateTimeFormat
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      if (Option(a).isDefined && Option(b).isDefined)
        scala.math.log10(1 + scala.math.abs(Days.daysBetween(formatter.parseDateTime(a), formatter.parseDateTime(b)).getDays))
      else
        0.0
    }
    val getVectorElem: (Int, Int) => UserDefinedFunction = (roundTo: Int, position: Int) =>
      udf((a: org.apache.spark.ml.linalg.Vector) =>
        BigDecimal(a.toArray(position)).setScale(roundTo, BigDecimal.RoundingMode.HALF_EVEN))
  }
  val UDF = new UDFClass

  class ClassificationMetrics(data: DataFrame) {
    val dataRDD: RDD[(Double, Double)] = data.rdd.map { case Row(label: Double, prediction: Double) => (label, prediction) }
    val metrics = new MulticlassMetrics(dataRDD)

    val accuracy: Double = metrics.accuracy
    val labels: Array[Double] = metrics.labels
    val precision: Array[Double] = labels.map(r => metrics.precision(r))
    val recall: Array[Double] = labels.map(r => metrics.recall(r))
    val f1: Array[Double] = labels.map(r => metrics.fMeasure(r))
    val confusionMatrix: Matrix = metrics.confusionMatrix

    def getConfusion: List[String] = confusionMatrix.transpose.toArray.grouped(confusionMatrix.numCols).toList.map(line => line.mkString(" "))

    // make it more presentable
    def printAll: Unit = {
      println(s"Accuracy: $accuracy")
      println(s"No of labels ${labels.length}")
      println(s"precision: ${precision.mkString("\t")}")
      println(s"recall: ${recall.mkString("\t")}")
      println(s"f1: ${f1.mkString("\t")}")
      println(s"Confusion Matrix :")
      getConfusion.foreach(println(_))
    }
  }

}
