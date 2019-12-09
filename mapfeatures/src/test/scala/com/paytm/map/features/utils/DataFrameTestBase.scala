package com.paytm.map.features.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit
import org.scalatest.{FunSpec, Matchers}

trait DataFrameTestBase extends FunSpec with Matchers with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "4")
  }

  def assertDataFrameEqualsUnordered[T](expected: Dataset[T], actual: Dataset[T]): Unit = {
    val onlyInActual = actual.except(expected)
    val onlyInExpected = expected.except(actual)
    val diff = onlyInActual.withColumn("Diff", lit("only in actual"))
      .union(onlyInExpected.withColumn("Diff", lit("only in expected")))
      .select("Diff", expected.columns: _*)

    if (diff.count != 0)
      diff.show(false)

    assert(diff.count(), 0)
  }

}
