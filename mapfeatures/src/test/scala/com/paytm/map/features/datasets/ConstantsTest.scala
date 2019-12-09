package com.paytm.map.features.datasets

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FunSpec, Matchers}
import com.paytm.map.features.datasets.Constants.{standarizePIN, standarizeVersion}

class ConstantsTest extends FunSpec with Matchers with DataFrameSuiteBase {

  describe("standarizePIN") {

    it("should return valid pin unaffected") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq("123456").toDF("pin_code")
      val outputDF = inputDF.withColumn("pin_code", standarizePIN($"pin_code"))

      assertDataFrameEquals(inputDF, outputDF)
    }

    it("should return null for pins that are not 6 characters") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq("123").toDF("pin_code")
      val outputDF = inputDF.withColumn("pin_code", standarizePIN($"pin_code"))

      val expectedDF = Seq(null.asInstanceOf[String]).toDF("pin_code")

      assertDataFrameEquals(expectedDF, outputDF)
    }

    it("should return null for pins that contain non-numeric characters") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq("12345a").toDF("pin_code")
      val outputDF = inputDF.withColumn("pin_code", standarizePIN($"pin_code"))

      val expectedDF = Seq(null.asInstanceOf[String]).toDF("pin_code")

      assertDataFrameEquals(expectedDF, outputDF)
    }
  }

  describe("standarizeVersion") {

    it("should return valid version unaffected") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq("1", "1.7", "1.7.0", "1.7.999", "1.7.0.2").toDF("version")
      val outputDF = inputDF.withColumn("version", standarizeVersion($"version"))

      assertDataFrameEquals(inputDF, outputDF)
    }

    it("should return unknown for versions that are not compatible") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq("spidey", "!@#!$", "{46.5.9}", "2a0f7mar28y", "2)and (select*fro.").toDF("version")
      val outputDF = inputDF.withColumn("version", standarizeVersion($"version"))

      val expectedDF = Seq.fill(inputDF.count.toInt)("unknown").toDF("version")

      assertDataFrameEquals(expectedDF, outputDF)
    }
  }

}
