package com.paytm.map.features.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.paytm.map.features.utils.DataframeMetrics.{ColumnStatistics, TableStatistics}
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpec, Ignore, Matchers}

@Ignore
class DataframeMetricsTest extends FunSpec with Matchers with DataFrameSuiteBase {

  def testDF(implicit sqlCtx: SQLContext) = {
    import sqlCtx.implicits._
    Seq(
      (Option(1), Option("a"), Option(Seq("a", "b"))),
      (Option(2), Option("b"), Option(Seq("a"))),
      (Option(2), Option(""), Option(Seq.empty[String])),
      (Option.empty[Int], Option.empty[String], Option.empty[Seq[String]])
    ).toDF("numeric_column", "string_column", "array_column").cache
  }

  describe("getTableMetrics") {

    it("should return two columns (summary and table_metrics)") {
      val tableStats = DataframeMetrics.getTableMetrics(testDF)
      tableStats.columns.toSet should equal(Set("summary", "table_metrics"))
    }

    it("should two values in the summary columns and the correct metric associated with the value") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val expected = Map((TableStatistics.TotalCount -> 4L), (TableStatistics.NumberOfColumns -> 3L))
      val tableStats = DataframeMetrics.getTableMetrics(testDF).cache
      TableStatistics.metrics.map { metric =>
        val value = tableStats.where($"summary" <=> metric.name).select("table_metrics").head.getAs[String](0).toLong
        value should equal(expected(metric.name))
      }
    }

  }

  describe("getColumnMetrics") {
    def columnStats(sqlCtx: SQLContext) = {
      import sqlCtx.implicits._
      import DataframeMetrics.TypeTagCast
      val data = DataframeMetrics.getColumnMetrics(testDF, Seq("numeric_column")).select("summary", "numeric_column").collect()
        .map {
          row =>
            val summary = row.getAs[String](0)
            val value = row.getAs[String](1)
            (summary, value)
        }.toMap

      ColumnStatistics.metrics.map {
        metric =>
          (metric.name, data(metric.name).fromTypeTag(metric.dataType))
      }.toMap
    }

    describe("numeric columns") {
      it("should return the summary column and all columns in the dataframe") {
        val columns = DataframeMetrics.getColumnMetrics(testDF, testDF.columns).columns.toSet
        columns should equal(Set("summary", "numeric_column", "string_column", "array_column"))
      }

      it("should return a summary for countNull for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountNull)
        value should equal(1L)
      }

      it("should return null countEmpty for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountEmpty)
        value should equal(0L)
      }

      it("should return a summary for countDistinct for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountDistinct)
        value should equal(2L)
      }

      it("should return a summary for min for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Min)
        value should equal(1.0)
      }

      it("should return a summary for max for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Max)
        value should equal(2.0)
      }

      it("should return a summary for sum for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Sum)
        value should equal(5.0)
      }

      it("should return a summary for avg for numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Average)
        value should equal(5.0 / 3)
      }

      it("should return a summary for avg length for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.AverageLength)
        value should equal(0.0)
      }
    }

    describe("string columns") {
      def columnStats(sqlCtx: SQLContext) = {
        import sqlCtx.implicits._
        import DataframeMetrics.TypeTagCast
        val data = DataframeMetrics.getColumnMetrics(testDF, Seq("string_column")).select("summary", "string_column").collect()
          .map {
            row =>
              val summary = row.getAs[String](0)
              val value = row.getAs[String](1)
              (summary, value)
          }.toMap

        ColumnStatistics.metrics.map {
          metric =>
            (metric.name, data(metric.name).fromTypeTag(metric.dataType))
        }.toMap
      }
      it("should return a summary for countNull for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountNull)
        value should equal(1L)
      }
      it("should return a summary for countEmpty for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountEmpty)
        value should equal(1L)
      }
      it("should return a summary for countDistinct for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountDistinct)
        value should equal(3L)
      }
      it("should return a summary for min for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Min)
        value should equal(0.0)
      }
      it("should return a summary for max for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Max)
        value should equal(0.0)
      }
      it("should return a summary for avg for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Average)
        value should equal(0.0)
      }

      it("should return a summary for avg length for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.AverageLength)
        value should equal(0.0)
      }
    }

    describe("array columns") {
      def columnStats(sqlCtx: SQLContext) = {
        import sqlCtx.implicits._
        import DataframeMetrics.TypeTagCast
        val data = DataframeMetrics.getColumnMetrics(testDF, Seq("array_column")).select("summary", "array_column").collect()
          .map {
            row =>
              val summary = row.getAs[String](0)
              val value = row.getAs[String](1)
              (summary, value)
          }.toMap

        ColumnStatistics.metrics.map {
          metric =>
            (metric.name, data(metric.name).fromTypeTag(metric.dataType))
        }.toMap
      }
      it("should return a summary for countNull for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountNull)
        value should equal(1L)
      }
      it("should return a summary for countEmpty for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountEmpty)
        value should equal(0L)
      }
      it("should return a summary for countDistinct for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.CountDistinct)
        value should equal(3L)
      }
      it("should return a summary for min for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Min)
        value should equal(0.0)
      }
      it("should return a summary for max for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Max)
        value should equal(0.0)
      }
      it("should return a summary for avg for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.Average)
        value should equal(0.0)
      }

      it("should return a summary for avg length for non-numeric columns") {
        val sqlCtx = sqlContext
        val colStats = columnStats(sqlCtx)
        val value = colStats(ColumnStatistics.AverageLength)
        value should equal(1.0)
      }
    }
  }
}
