package com.paytm.map.features.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.scalatest.{FunSpec, Ignore, Matchers}

@Ignore
class DataframeDeltaTest extends FunSpec with Matchers with DataFrameSuiteBase {

  describe("Table Delta") {
    it("should return empty col if both dataframes match") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val currDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val delta = DataframeDelta.TableDelta(prevDF, currDF)
      delta.deletedFields should equal(Seq.empty[String])
      delta.insertedFields should equal(Seq.empty[String])
    }

    it("should return a_number col as inserted") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq(("a"), ("a")).toDF("a_letter")
      val currDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val delta = DataframeDelta.TableDelta(prevDF, currDF)
      delta.deletedFields should equal(Seq.empty[String])
      delta.insertedFields should equal(Seq("a_number"))
    }

    it("should return a_number col as deleted") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val currDF = Seq(("a"), ("a")).toDF("a_letter")
      val delta = DataframeDelta.TableDelta(prevDF, currDF)
      delta.deletedFields should equal(Seq("a_number"))
      delta.insertedFields should equal(Seq.empty[String])
    }

    it("should return a_number col as inserted and deleted if the type is changed") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq(("1.0", "a"), ("1.1", "a")).toDF("a_number", "a_letter")
      val currDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val delta = DataframeDelta.TableDelta(prevDF, currDF)
      delta.deletedFields should equal(Seq("a_number"))
      delta.insertedFields should equal(Seq("a_number"))
    }

    it("should return all columns as added with an empty dataframe") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val prevDF = sqlContext.emptyDataFrame
      val currDF = Seq((1.0, "a"), (1.1, "a")).toDF("a_number", "a_letter")
      val delta = DataframeDelta.TableDelta(prevDF, currDF)
      delta.deletedFields should equal(Seq.empty[String])
      delta.insertedFields should equal(Seq("a_number", "a_letter"))
    }
  }

  describe("Row Delta") {

    def emptyDF(implicit sqlCtx: SQLContext): DataFrame = {
      import sqlCtx.implicits._
      Seq.empty[(Option[Long], Option[Long])].toDF("primary_key_1", "primary_key_2")
    }

    it("should return empty dataframes for both deleted and inserted with the same primary key exist") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L), (2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val currDF = Seq((1L, 1L), (2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val delta = DataframeDelta.RowDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))

      assertDataFrameEquals(delta.deletedRows, emptyDF)
      assertDataFrameEquals(delta.insertedRows, emptyDF)
    }

    it("should return only deletedRows dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L), (2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val currDF = Seq((2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val delta = DataframeDelta.RowDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))

      assertDataFrameEquals(delta.deletedRows, Seq((Option(1L), Option(1L))).toDF("primary_key_1", "primary_key_2"))
      assertDataFrameEquals(delta.insertedRows, emptyDF)
    }

    it("should return only insertedRows dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val currDF = Seq((1L, 1L), (2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val delta = DataframeDelta.RowDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))

      assertDataFrameEquals(delta.deletedRows, emptyDF)
      assertDataFrameEquals(delta.insertedRows, Seq((Option(1L), Option(1L))).toDF("primary_key_1", "primary_key_2"))
    }

    it("should throw an exception when primary keys don't exist in the current dataframe") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((2L, 2L)).toDF("primary_key_1", "primary_key_2")
      val currDF = Seq((1L, 1L), (2L, 2L)).toDF("not_primary_key_1", "not_primary_key_2")
      intercept[AssertionError] {
        DataframeDelta.RowDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      }
    }

    it("should throw an exception when primary keys don't exist in the prev dataframe") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((2L, 2L)).toDF("not_primary_key_1", "not_primary_key_2")
      val currDF = Seq((1L, 1L), (2L, 2L)).toDF("primary_key_1", "primary_key_2")
      intercept[AssertionError] {
        DataframeDelta.RowDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      }
    }
  }

  describe("Column Delta") {
    def emptyDF(implicit sqlCtx: SQLContext) = {
      import sqlCtx.implicits._
      Seq.empty[(Long, Long, Array[String])].toDF("primary_key_1", "primary_key_2", "columns_with_change")
    }

    it("should return an empty dataframe when there is no change bewteen the prev and curr dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a"), (2L, 2L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "a"), (2L, 2L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      assertDataFrameEquals(delta.updatedColumns, emptyDF)
    }

    it("should return updates on the shared columns between prev and curr dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a"), (2L, 2L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "a"), (2L, 2L, "c")).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedChange = Seq((2L, 2L, Seq("col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }

    it("should return updates on the multiple shared columns between prev and curr dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a", 1.0), (2L, 2L, "b", 2.0)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
      val currDF = Seq((1L, 1L, "a", 1.0), (2L, 2L, "c", 2.5)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedChange = Seq((2L, 2L, Seq("col2", "col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }

    it("should ignore columns that exist only in either of the dataframes") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "old", "a"), (2L, 2L, "old", "b")).toDF("primary_key_1", "primary_key_2", "old_column", "col1")
      val currDF = Seq((1L, 1L, "new", "a"), (2L, 2L, "new", "c")).toDF("primary_key_1", "primary_key_2", "new_column", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedChange = Seq((2L, 2L, Seq("col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }

    it("should return no update if the columns are compared approximately") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, 0.1), (2L, 2L, 0.2)).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, 0.2), (2L, 2L, 0.3)).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"), 0.1)
      assertDataFrameEquals(delta.updatedColumns, emptyDF)
    }

    it("should return update if the columns in comparisoon exceed the tolerance") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, 0.1), (2L, 2L, 0.2)).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, 0.2), (2L, 2L, 0.31)).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"), 0.1)
      val expectedChange = Seq((2L, 2L, Seq("col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }

    it("should be able to handle array types") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, Array(0.1, 0.1)), (2L, 2L, Array(0.2, 0.2))).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, Array(0.2, 0.2)), (2L, 2L, Array(0.3, 0.3))).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"), 0.1)
      assertDataFrameEquals(delta.updatedColumns, emptyDF)
    }

    it("should be able to handle array of different lengths as mismatch") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, Array(0.1)), (2L, 2L, Array(0.2))).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, Array(0.2, 0.2)), (2L, 2L, Array(0.3, 0.3))).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"), 0.1)
      val expectedChange = Seq((1L, 1L, Seq("col1")), (2L, 2L, Seq("col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }

    it("should return changes when only one element in the array is not equal") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, Array(0.1, 0.1)), (2L, 2L, Array(0.2, 0.2))).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, Array(0.2, 0.3)), (2L, 2L, Array(0.4, 0.3))).toDF("primary_key_1", "primary_key_2", "col1")
      val delta = DataframeDelta.ColumnDelta(prevDF, currDF, Seq("primary_key_1", "primary_key_2"), 0.1)
      val expectedChange = Seq((1L, 1L, Seq("col1")), (2L, 2L, Seq("col1"))).toDF("primary_key_1", "primary_key_2", "columns_with_change")
      assertDataFrameEquals(delta.updatedColumns, expectedChange)
    }
  }

  describe("Upsert Computation") {

    it("should return empty upsert dataframe when there is no change") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val emptyDF = Seq.empty[(Long, Long, Option[String], Array[String])].toDF("primary_key_1", "primary_key_2", "col1", "columns_with_change")
      val prevDF = Seq((1L, 1L, "a"), (2L, 2L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "a"), (2L, 2L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val upsert = DataframeDelta.computeUpsert(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      assertDataFrameEquals(upsert, emptyDF)
    }

    it("should return a upsert dataframe that has inserted and changed") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "c", 1.0), (2L, 2L, "b", 2.0)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
      val upsert = DataframeDelta.computeUpsert(prevDF, currDF, Seq("primary_key_1", "primary_key_2")).orderBy("primary_key_1")
      val expectedUpsert = Seq((1L, 1L, "c", 1.0, Seq("col2", "col1")), (2L, 2L, "b", 2.0, Seq[String]())).toDF("primary_key_1", "primary_key_2", "col1", "col2", "columns_with_change")
      assertDataFrameEquals(upsert, expectedUpsert)
    }

    it("should return a upsert dataframe that contains all data from currDF if prevDF has no record") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val currDF = Seq((1L, 1L, "c", 1.0), (2L, 2L, "b", 2.0)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
      val prevDF = currDF.sparkSession.createDataFrame(currDF.sparkSession.sparkContext.emptyRDD[Row], currDF.schema)
      val upsert = DataframeDelta.computeUpsert(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedUpsert = Seq((1L, 1L, "c", 1.0), (2L, 2L, "b", 2.0)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
        .withColumn("columns_with_change", lit(Array.empty[String]))
      assertDataFrameEquals(upsert, expectedUpsert)
    }

    it("should return a upsert dataframe that contains added new fields in the currDF if no columns had changed") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "a", 1.0)).toDF("primary_key_1", "primary_key_2", "col1", "col2")
      val upsert = DataframeDelta.computeUpsert(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedUpsert = Seq((1L, 1L, "a", 1.0, Seq("col2"))).toDF("primary_key_1", "primary_key_2", "col1", "col2", "columns_with_change")
      assertDataFrameEquals(upsert, expectedUpsert)
    }

    it("should return a upsert dataframe that contains added only fields that changed  if no columns are added") {
      val sqlCtx: SQLContext = sqlContext
      import sqlCtx.implicits._

      val prevDF = Seq((1L, 1L, "a")).toDF("primary_key_1", "primary_key_2", "col1")
      val currDF = Seq((1L, 1L, "b")).toDF("primary_key_1", "primary_key_2", "col1")
      val upsert = DataframeDelta.computeUpsert(prevDF, currDF, Seq("primary_key_1", "primary_key_2"))
      val expectedUpsert = Seq((1L, 1L, "b", Seq("col1"))).toDF("primary_key_1", "primary_key_2", "col1", "columns_with_change")
      assertDataFrameEquals(upsert, expectedUpsert)
    }
  }
}
