package com.paytm.map.features

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FunSpec, Matchers}
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame

class SalesAggregateTest extends FunSpec with Matchers with DataFrameSuiteBase {

  describe("lastOrderDateFeature aggregation in addSalesAggregates") {

    it("should aggregate properly in addSalesAggregates(...)") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val inputDF = Seq(("123", "2018-11-06", "BB", "20181105"), ("123", "2018-11-06", "BB", "20181101"), ("124", "2018-11-06", "AA", "20181104")).toDF("customer_id", "dt", "RU_Level", "created_at")
      val aggDF = inputDF
        .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "RU_Level", unFiltered = true)
        .renameColsWithSuffix("_last_attempt_order_date", excludedColumns = Seq("customer_id", "dt"))

      println("input DF after aggregations and renaming cols")
      aggDF.show()

      val expectedDF = Seq(("124", "2018-11-06", "20181104", null), ("123", "2018-11-06", null, "20181105")).toDF("customer_id", "dt", "AA_last_attempt_order_date", "BB_last_attempt_order_date")
      println("expectedDF")
      expectedDF.show()
      assertDataFrameEquals(aggDF, expectedDF)
    }
  }
}
