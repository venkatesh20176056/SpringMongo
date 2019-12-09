package com.paytm.map.features.Chakki

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}
import com.paytm.map.features.Chakki.JoinDatasets._

class JoinDatasetsTest extends FunSpec with Matchers with DataFrameSuiteBase {
  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  it("should remove non-numeric columns from group1 when adding to group4 base dataframe") {
    import spark.implicits._

    val group4BaseDF = Seq(Some(1)).toDF("customer_id")

    val group1Columns = Seq(
      new StructField("doubleTypeColumn", DataTypes.DoubleType, nullable = true),
      new StructField("intTypeColumn", DataTypes.IntegerType, nullable = true),
      new StructField("longTypeColumn", DataTypes.LongType, nullable = true),
      new StructField("binaryTypeColumn", DataTypes.BinaryType, nullable = true),
      new StructField("dateTypeColumn", DataTypes.DateType, nullable = true),
      new StructField("stringTypeColumn", DataTypes.StringType, nullable = true)
    )

    val resultSchema = group4BaseDF.addNumericColumnsFromGroup1(group1Columns, spark).schema

    val expectedSchema = new StructType()
      .add("customer_id", DataTypes.IntegerType, nullable = true)
      .add("doubleTypeColumn", DataTypes.DoubleType, nullable = true)
      .add("intTypeColumn", DataTypes.IntegerType, nullable = true)
      .add("longTypeColumn", DataTypes.LongType, nullable = true)

    assertTrue(resultSchema == expectedSchema)
  }

  it("should add new numeric(Double, Integer and Long type) columns with all zero values to group 4 base dataframe") {
    import spark.implicits._

    val baseDF = Seq((100, "dj"), (200, "pt"), (300, "mt"), (400, "gt")).toDF("customer_id", "name")

    val columnsToBeAdded = Seq(
      new StructField("doubleTypeColumn", DataTypes.DoubleType, nullable = true),
      new StructField("longTypeColumn", DataTypes.LongType, nullable = true),
      new StructField("intTypeColumn", DataTypes.IntegerType, nullable = true),
      new StructField("stringTypeColumn", DataTypes.StringType, nullable = true)
    )

    val result = baseDF.addNumericColumnsFromGroup1(columnsToBeAdded, spark)
    result.show(false)
    result.printSchema()

    val expected = Seq(
      TestClass1(100, "dj", null, null, null),
      TestClass1(200, "pt", null, null, null),
      TestClass1(300, "mt", null, null, null),
      TestClass1(400, "gt", null, null, null)
    )
      .toDF()

    expected.printSchema()
    assertDataFrameEquals(expected, result)
  }

  it("should remove duplicate numeric columns from g1 that already exist in g4 base dataframe") {
    import spark.implicits._

    val group4BaseDF = Seq((1, Some(200))).toDF("customer_id", "pincode")

    val group1Columns = Seq(
      new StructField("doubleTypeColumn", DataTypes.DoubleType, nullable = true),
      new StructField("pincode", DataTypes.IntegerType, nullable = true)
    )

    val result = group4BaseDF.addNumericColumnsFromGroup1(group1Columns, spark)

    val expected = Seq(TestClass2(1, Some(200), None))
      .toDF()

    assertDataFrameEquals(expected, result)
  }
}

case class TestClass1(customer_id: Int, name: String, doubleTypeColumn: Option[Double], longTypeColumn: Option[Long], intTypeColumn: Option[Int])
case class TestClass2(customer_id: Int, pincode: Option[Int], doubleTypeColumn: Option[Double])