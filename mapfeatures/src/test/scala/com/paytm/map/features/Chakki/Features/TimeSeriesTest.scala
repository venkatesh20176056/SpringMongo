package com.paytm.map.features.Chakki.Features

import com.paytm.map.features.Chakki.Features.TimeSeries.{nDays, nMonths}
import org.joda.time.DateTime
import org.scalatest.{FunSpec, Matchers}

class TimeSeriesTest extends FunSpec with Matchers {

  describe("TimeSeries") {
    import org.apache.spark.sql.functions.col

    it("should map column suffix to correct timeserie") {
      TimeSeries.getTimeSeries("some_feature_last_month") shouldBe nMonths(1).toString
      TimeSeries.getTimeSeries("some_feature_second_last_month") shouldBe nMonths(2).toString
      TimeSeries.getTimeSeries("some_feature_third_last_month") shouldBe nMonths(3).toString
      TimeSeries.getTimeSeries("some_feature_fourth_last_month") shouldBe nMonths(4).toString
      TimeSeries.getTimeSeries("some_feature_fifth_last_month") shouldBe nMonths(5).toString
      TimeSeries.getTimeSeries("some_feature_sixth_last_month") shouldBe nMonths(6).toString
    }

    it("should filter for third to last month") {
      val targetDate = new DateTime("2018-05-05")

      nMonths(1).filterCondn(targetDate) shouldBe col("dt").between("2018-04-01", "2018-04-30")
      nMonths(2).filterCondn(targetDate) shouldBe col("dt").between("2018-03-01", "2018-03-31")
      nMonths(3).filterCondn(targetDate) shouldBe col("dt").between("2018-02-01", "2018-02-28")
      nMonths(4).filterCondn(targetDate) shouldBe col("dt").between("2018-01-01", "2018-01-31")
      nMonths(5).filterCondn(targetDate) shouldBe col("dt").between("2017-12-01", "2017-12-31")
      nMonths(6).filterCondn(targetDate) shouldBe col("dt").between("2017-11-01", "2017-11-30")
    }

    it("should filter for only targetDate") {
      val targetDateStr = "2018-05-05"
      val targetDate = new DateTime(targetDateStr)

      nDays(1).filterCondn(targetDate) shouldBe col("dt").between(targetDateStr, targetDateStr)
    }
  }
}
