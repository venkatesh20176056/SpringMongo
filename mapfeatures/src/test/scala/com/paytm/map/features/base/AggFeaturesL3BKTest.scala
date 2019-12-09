package com.paytm.map.features.base

import java.sql.{Date, Timestamp}

import com.paytm.map.features.base.AggFeaturesL3BK._
import com.paytm.map.features.base.BaseTableUtils.{OperatorCnt, TravelDetail}
import com.paytm.map.features.utils.ArgsUtils.formatter
import com.paytm.map.features.utils.DataFrameTestBase
import com.paytm.map.features.base.DataTables.DFCommons
import org.apache.spark.sql.functions.explode

class AggFeaturesL3BKTest extends DataFrameTestBase {

  val customerId1 = 12345L
  val customerId2 = 23456L
  val date1Sat = "2018-11-10"
  val date2Sun = "2018-11-11"
  val date3Mon = "2018-11-12"
  val orderId1 = "1234"
  val orderId2 = "2345"
  val orderId3 = "3456"
  val orderId4 = "4567"
  val destination1 = "Mexico"
  val destination2 = "France"

  describe("flight travel features") {
    import spark.sqlContext.implicits._

    it("should add preferable travel day and booking day ranked by number of distinct order ids") {
      val travelOlap = Seq(
        (customerId1, orderId1, date1Sat, date1Sat, date1Sat),
        (customerId1, orderId1, date1Sat, date1Sat, date1Sat),
        (customerId1, orderId1, date1Sat, date1Sat, date2Sun),
        (customerId1, orderId2, date1Sat, date1Sat, date2Sun),
        (customerId2, orderId3, date1Sat, date1Sat, date2Sun),
        (customerId2, orderId4, date2Sun, date2Sun, date2Sun)
      ).toDF("customer_id", "order_id", "dt", "created_at", "travel_date")

      val expected = Seq(
        (customerId1, date1Sat, Array(OperatorCnt("Sun", 2), OperatorCnt("Sat", 1)), Array(OperatorCnt("Sat", 3))),
        (customerId2, date1Sat, Array(OperatorCnt("Sun", 1)), Array(OperatorCnt("Sat", 1))),
        (customerId2, date2Sun, Array(OperatorCnt("Sun", 1)), Array(OperatorCnt("Sun", 1)))
      ).toDF("customer_id", "dt", "FL_preferable_travel_day", "FL_preferable_booking_day")

      val actual = travelFeatures(travelOlap, "FL")(spark)
        .select("customer_id", "dt", "FL_preferable_travel_day", "FL_preferable_booking_day")

      assertDataFrameEqualsUnordered(expected, actual)
    }

  }

  describe("next travel features") {
    import spark.sqlContext.implicits._

    it("should add next depart date and destination country within next 6 months") {
      val startDate = formatter.parseDateTime(date1Sat)
      val targetDate = formatter.parseDateTime(date3Mon)
      val tripType = "International"
      val travelOlap = Seq(
        (tripType, customerId1, date1Sat, date2Sun, destination1),
        (tripType, customerId1, date1Sat, date3Mon, destination2),
        (tripType, customerId2, date2Sun, date1Sat, destination2)
      ).toDF("trip_type", "customer_id", "dt", "travel_date", "destination_country")

      val expected = Seq(
        (customerId1, date1Sat, date2Sun, destination1),
        (customerId1, date2Sun, date3Mon, destination2),
        (customerId1, date3Mon, null, null),
        (customerId2, date1Sat, null, null),
        (customerId2, date2Sun, null, null),
        (customerId2, date3Mon, null, null)
      ).toDF("customer_id", "dt", "IFL_next_travel_date", "IFL_next_destination_country")

      val actual = nextTravelFeatures(travelOlap, startDate, targetDate, "IFL")(spark)
        .select("customer_id", "dt", "IFL_next_travel_date", "IFL_next_destination_country")

      assertDataFrameEqualsUnordered(expected, actual)

    }
  }

  describe("future travel features") {
    import spark.sqlContext.implicits._

    it("should column with future destination and date") {
      val destinationA = "A"
      val destinationB = "B"
      val destinationC = "C"
      val startDate = formatter.parseDateTime(date1Sat)
      val targetDate = formatter.parseDateTime(date3Mon)
      val date2SunTs = Timestamp.valueOf(s"$date2Sun 00:00:00")
      val date3MonTs = Timestamp.valueOf(s"$date3Mon 00:00:00")

      val travelOlap = Seq(
        (customerId1, orderId1, Date.valueOf(date1Sat), Date.valueOf(date1Sat), Date.valueOf(date1Sat), destinationA),
        (customerId1, orderId1, Date.valueOf(date1Sat), Date.valueOf(date1Sat), Date.valueOf(date1Sat), destinationA),
        (customerId1, orderId1, Date.valueOf(date1Sat), Date.valueOf(date1Sat), Date.valueOf(date2Sun), destinationB),
        (customerId1, orderId2, Date.valueOf(date1Sat), Date.valueOf(date1Sat), Date.valueOf(date2Sun), destinationC),
        (customerId2, orderId3, Date.valueOf(date1Sat), Date.valueOf(date1Sat), Date.valueOf(date2Sun), destinationA),
        (customerId2, orderId4, Date.valueOf(date2Sun), Date.valueOf(date2Sun), Date.valueOf(date3Mon), destinationB)
      ).toDF("customer_id", "order_id", "dt", "created_at", "travel_date", "destination_city")

      val actual = futureTravelFeatures(travelOlap, startDate, targetDate, "FL")(spark)
        .select("customer_id", "dt", "FL_future_travel_details")

      val expected = Seq(
        (customerId1, Date.valueOf(date1Sat), Array(
          TravelDetail(destinationB, date2SunTs),
          TravelDetail(destinationC, date2SunTs)
        )),
        (customerId2, Date.valueOf(date1Sat), Array(
          TravelDetail(destinationA, date2SunTs),
          TravelDetail(destinationB, date3MonTs)
        )),
        (customerId2, Date.valueOf(date2Sun), Array(
          TravelDetail(destinationB, date3MonTs)
        ))
      ).toDF("customer_id", "dt", "FL_future_travel_details")
        .alignSchema(actual.schema)

      assertDataFrameEqualsUnordered( //expected.orderBy($"customer_id", $"dt"), actual.orderBy($"customer_id", $"dt"))
        expected.withColumn("FL_future_travel_details", explode($"FL_future_travel_details")),
        actual.withColumn("FL_future_travel_details", explode($"FL_future_travel_details"))
      )
    }
  }
}
