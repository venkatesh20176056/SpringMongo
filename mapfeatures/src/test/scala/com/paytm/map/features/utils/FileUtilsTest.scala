package com.paytm.map.features.utils

import java.nio.file.Files

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.scalatest.{FlatSpec, Matchers}

class FileUtilsTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  it should "return none if no success file is found" in {
    val testPath = Files.createTempDirectory("mapfeatures")
    val someDates = FileUtils.getLatestTableDate("file://" + testPath.toString, spark, "2018-02-11")
    someDates should equal(None)
  }

  it should "return some dates if a success file is found" in {
    val sqlCtx: SQLContext = sqlContext
    import sqlCtx.implicits._
    val testPath = "file://" + Files.createTempDirectory("mapfeatures").toString
    val testPathWithDate = testPath + "/dt=2018-02-01"
    Seq(1L, 2L).toDF("customer_id").write.mode(SaveMode.Overwrite).parquet(testPathWithDate)
    val someDates = FileUtils.getLatestTableDate(testPath, spark, "2018-02-11", "dt=")
    someDates should equal(Some("2018-02-01"))
  }

  it should "return the latest dates before the end date" in {
    val sqlCtx: SQLContext = sqlContext
    import sqlCtx.implicits._
    val testPath = "file://" + Files.createTempDirectory("mapfeatures").toString

    Seq("2018-02-01", "2018-02-02", "2018-02-03", "2018-02-12").foreach { date =>
      val testPathWithDate = testPath + "/dt=" + date
      Seq(1L, 2L).toDF("customer_id").write.mode(SaveMode.Overwrite).parquet(testPathWithDate)
    }
    val someDates = FileUtils.getLatestTableDate(testPath, spark, "2018-02-11", "dt=")
    someDates should equal(Some("2018-02-03"))
  }

  it should "return the latest dates" in {
    val sqlCtx: SQLContext = sqlContext
    import sqlCtx.implicits._
    val testPath = "file://" + Files.createTempDirectory("mapfeatures").toString

    Seq("2018-02-01", "2018-02-02", "2018-02-03", "2018-02-12").foreach { date =>
      val testPathWithDate = testPath + "/dt=" + date
      Seq(1L, 2L).toDF("customer_id").write.mode(SaveMode.Overwrite).parquet(testPathWithDate)
    }
    val someDates = FileUtils.getLatestTableDateStringOption(testPath, spark, "dt=")
    someDates should equal(Some("2018-02-12"))
  }
}
