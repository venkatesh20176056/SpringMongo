package com.paytm.map.features.utils

import com.paytm.map.features.CommonSparkSpecHelper
import com.paytm.map.features.utils.GroupedDataframeOperations._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FunSuite, Ignore, Matchers}

class GroupedDataframeOperationsSpec extends FunSuite with CommonSparkSpecHelper with Matchers {

  test("getColumnsUsingComparator should return the correct values (select column has no prefix)") {
    val verticals = Seq("AA", "BB").map("RU_" + _)
    val key = SimpleColumn("customer_id", DataTypes.IntegerType)
    val comparisonColumn = SimpleColumn("count_of_txns", DataTypes.IntegerType)
    val selectColumns = Seq(SelectColumn("timestamp", DataTypes.IntegerType, false))
    val schema = StructType(Seq(
      StructField("customer_id", DataTypes.IntegerType),
      StructField("RU_AA_count_of_txns", DataTypes.IntegerType),
      StructField("RU_BB_count_of_txns", DataTypes.IntegerType),
      StructField("timestamp", DataTypes.IntegerType)
    ))
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val encoder = RowEncoder(schema)
    val group = Iterator(
      new GenericRowWithSchema(Array(1, 1, 2, 4), schema),
      new GenericRowWithSchema(Array(1, 5, 1, 10), schema)
    )

    val res = getColumnsUsingComparator[Int](key, group, verticals, "RU", comparisonColumn, selectColumns, GTE[Int])
    val expected = Seq(1, 10, 4)
    res.toSeq should contain theSameElementsInOrderAs expected
  }

  test("getColumnsUsingComparator should return the correct values (select column has prefix)") {
    val verticals = Seq("AA", "BB").map("RU_" + _)
    val key = SimpleColumn("customer_id", DataTypes.IntegerType)
    val comparisonColumn = SimpleColumn("count_of_txns", DataTypes.IntegerType)
    val selectColumns = Seq(SelectColumn("timestamp", DataTypes.IntegerType, true))
    val schema = StructType(Seq(
      StructField("customer_id", DataTypes.IntegerType),
      StructField("RU_AA_count_of_txns", DataTypes.IntegerType),
      StructField("RU_BB_count_of_txns", DataTypes.IntegerType),
      StructField("RU_AA_timestamp", DataTypes.IntegerType),
      StructField("RU_BB_timestamp", DataTypes.IntegerType)
    ))
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val encoder = RowEncoder(schema)
    val group = Iterator(
      new GenericRowWithSchema(Array(1, 1, 2, 4, 100), schema),
      new GenericRowWithSchema(Array(1, 5, 1, 10, 200), schema)
    )

    val res = getColumnsUsingComparator[Int](key, group, verticals, "RU", comparisonColumn, selectColumns, GTE[Int])
    val expected = Seq(1, 10, 100)
    res.toSeq should contain theSameElementsInOrderAs expected
  }

  test("getColumnsUsingComparator should return the correct values (multiple select columns with prefix)") {
    val verticals = Seq("AA", "BB").map("RU_" + _)
    val key = SimpleColumn("customer_id", DataTypes.IntegerType)
    val comparisonColumn = SimpleColumn("count_of_txns", DataTypes.IntegerType)
    val selectColumns = Seq(SelectColumn("timestamp", DataTypes.IntegerType, true), SelectColumn("platform", DataTypes.StringType, true))
    val schema = StructType(Seq(
      StructField("customer_id", DataTypes.IntegerType),
      StructField("RU_AA_count_of_txns", DataTypes.IntegerType),
      StructField("RU_BB_count_of_txns", DataTypes.IntegerType),
      StructField("RU_AA_timestamp", DataTypes.IntegerType),
      StructField("RU_BB_timestamp", DataTypes.IntegerType),
      StructField("RU_AA_platform", DataTypes.StringType),
      StructField("RU_BB_platform", DataTypes.StringType)
    ))
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val encoder = RowEncoder(schema)
    val group = Iterator(
      new GenericRowWithSchema(Array(1, 1, 2, 4, 100, "apple", "android"), schema),
      new GenericRowWithSchema(Array(1, 5, 1, 10, 200, "apple", "android"), schema)
    )

    val res = getColumnsUsingComparator[Int](key, group, verticals, "RU", comparisonColumn, selectColumns, GTE[Int])
    val expected = Seq(1, 10, "apple", 100, "android")
    res.toSeq should contain theSameElementsInOrderAs expected
  }

  test("getColumnsUsingComparitor should return the correct values (select column has no prefix, result columns renamed)") {
    val verticals = Seq("RU_AA")
    val key = SimpleColumn("customer_id", DataTypes.IntegerType)
    val comparisonColumn = SimpleColumn("count_of_txns", DataTypes.IntegerType)
    val selectColumns = Seq(SelectColumn("timestamp", DataTypes.IntegerType, false, "ts"))
    val schema = StructType(Seq(
      StructField("customer_id", DataTypes.IntegerType),
      StructField("RU_AA_count_of_txns", DataTypes.IntegerType),
      StructField("timestamp", DataTypes.IntegerType)
    ))
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val encoder = RowEncoder(schema)
    val group = Iterator(
      new GenericRowWithSchema(Array(1, 1, 4), schema),
      new GenericRowWithSchema(Array(1, 5, 10), schema)
    )

    val res = getColumnsUsingComparator[Int](key, group, verticals, "RU", comparisonColumn, selectColumns, GTE[Int])
    val expected = Seq("customer_id", "RU_AA_ts")
    res.schema.map(_.name) should contain theSameElementsInOrderAs expected
  }

  test("getColumnsUsingComparator should return the correct values (additional select columns for a subset of verticals)") {
    val specialVerticals = Seq("RU_AA")
    val verticals = Seq("AA", "BB").map("RU_" + _)
    val key = SimpleColumn("customer_id", DataTypes.IntegerType)
    val comparisonColumn = SimpleColumn("count_of_txns", DataTypes.IntegerType)
    val selectColumns = Seq(SelectColumn("timestamp", DataTypes.IntegerType, true))
    val additionalColumns = AdditionalSelectColumn(specialVerticals, Seq(SelectColumn("special_select_column", DataTypes.IntegerType, true)))
    val schema = StructType(Seq(
      StructField("customer_id", DataTypes.IntegerType),
      StructField("RU_AA_count_of_txns", DataTypes.IntegerType),
      StructField("RU_BB_count_of_txns", DataTypes.IntegerType),
      StructField("RU_AA_special_select_column", DataTypes.IntegerType),
      StructField("RU_AA_timestamp", DataTypes.IntegerType),
      StructField("RU_BB_timestamp", DataTypes.IntegerType)
    ))
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    implicit val encoder = RowEncoder(schema)
    val group = Iterator(
      new GenericRowWithSchema(Array(1, 1, 2, 11, 4, 100), schema),
      new GenericRowWithSchema(Array(1, 5, 1, 11, 10, 200), schema)
    )

    val res = getColumnsUsingComparator[Int](key, group, verticals, "RU", comparisonColumn, selectColumns, GTE[Int], additionalColumns)
    val expected = Seq(1, 10, 11, 100)
    res.toSeq should contain theSameElementsInOrderAs expected
  }

}
