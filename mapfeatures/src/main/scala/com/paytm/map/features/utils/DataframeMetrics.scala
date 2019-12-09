package com.paytm.map.features.utils

import com.paytm.map.features.Metrics.Metric
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.reflect.runtime.universe._

object DataframeMetrics {

  implicit class TypeTagCast(a: String) {
    import scala.util.Try
    val longType = typeTag[Long].tpe
    val intType = typeTag[Int].tpe
    val doubleType = typeTag[Double].tpe
    val stringType = typeTag[String].tpe

    def fromTypeTag[A](tt: TypeTag[A]): A = {
      tt.tpe match {
        case `longType`   => Try(a.toLong).getOrElse(0L).asInstanceOf[A]
        case `intType`    => Try(a.toInt).getOrElse(0).asInstanceOf[A]
        case `doubleType` => Try(a.toDouble).getOrElse(0.0).asInstanceOf[A]
        case `stringType` => a.asInstanceOf[A]
      }
    }
  }

  object TableStatistics {
    val TotalCount = "total_count"
    val NumberOfColumns = "number_of_columns"
    val metrics = Seq(Metric(TotalCount, typeTag[Long]), Metric(NumberOfColumns, typeTag[Int]))
  }

  object ColumnStatistics {
    val CountNull = "count_null"
    val CountDistinct = "count_distinct"
    val CountEmpty = "count_empty"
    val Min = "min"
    val Max = "max"
    val Sum = "sum"
    val Average = "avg"
    val AverageLength = "avg_length"

    val metrics = Seq(
      Metric(CountNull, typeTag[Long]),
      Metric(CountDistinct, typeTag[Long]),
      Metric(CountEmpty, typeTag[Long]),
      Metric(Min, typeTag[Double]),
      Metric(Max, typeTag[Double]),
      Metric(Sum, typeTag[Double]),
      Metric(Average, typeTag[Double]),
      Metric(AverageLength, typeTag[Double])
    )
  }

  def getTableMetrics(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import TableStatistics._
    Seq((TotalCount, df.count.toString), (NumberOfColumns, df.columns.length.toString)).toDF("summary", "table_metrics")
  }

  def getColumnMetrics(df: DataFrame, columns: Seq[String]): DataFrame = {
    val numericColumnMap = df.schema.map { field =>
      field.dataType match {
        case _: NumericType => (field.name -> df(field.name))
        case _              => (field.name -> lit(null))
      }
    }.toMap

    val arrayColumnMap = df.schema.map { field =>
      field.dataType match {
        case _: ArrayType => (field.name -> df(field.name))
        case _            => (field.name -> lit(null))
      }
    }.toMap

    import ColumnStatistics._
    val statistics = Seq(
      (CountNull -> columns.map(c => count(when(df(c).isNull, 1)).cast(StringType).as(c))),
      (CountDistinct -> columns.map(c => countDistinct(df(c)).cast(StringType).as(c))),
      (CountEmpty -> columns.map(c => count(when(df(c).cast(StringType) <=> "", 1)).cast(StringType).as(c))),
      (Min -> columns.map(c => min(df(c)).cast(StringType).as(c))),
      (Max -> columns.map(c => max(df(c)).cast(StringType).as(c))),
      (Sum -> columns.map(c => sum(numericColumnMap(c)).cast(StringType).as(c))),
      (Average -> columns.map(c => avg(numericColumnMap(c)).cast(StringType).as(c))),
      (AverageLength -> columns.map(c => avg(when(arrayColumnMap(c) isNotNull, size(arrayColumnMap(c))).otherwise(lit(null))).cast(StringType).as(c)))
    )

    val result = statistics.map {

      case (statsName, cols) =>
        val stats = cols.grouped(1000).flatMap {
          cols =>
            df.select(cols: _*).head().toSeq
        }
        Row(statsName :: stats.toList: _*)
    }.toList

    val schema = StructType(StructField("summary", StringType) :: columns.map(StructField(_, StringType)).toList)
    df.sparkSession.createDataFrame(df.sparkSession.sparkContext.makeRDD[Row](result), schema)
  }
}
