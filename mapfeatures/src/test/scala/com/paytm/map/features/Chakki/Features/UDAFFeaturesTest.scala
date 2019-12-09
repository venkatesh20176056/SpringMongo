package com.paytm.map.features.Chakki.Features

import java.sql.Date

import com.paytm.map.features.Chakki.FeatChakki.FeatExecutor
import com.paytm.map.features.Chakki.Features.TimeSeries.All
import com.paytm.map.features.utils.DataFrameTestBase
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

class UDAFFeaturesTest extends DataFrameTestBase {

  describe("nested max feat") {

    it("should create aggregate function that collects column as set and apply groupBy and max within the set") {
      import spark.sqlContext.implicits._

      val d1 = Date.valueOf("2019-02-01")
      val d2 = Date.valueOf("2019-02-02")

      val df = Seq(
        ("c1", d1, Seq(("g1", 0), ("g2", 1))),
        ("c1", d2, Seq(("g1", 1), ("g1", 2), ("g2", 2))),
        ("c2", d1, Seq(("g1", 1))),
        ("c2", d2, Seq(("g1", 2)))
      )
        .toDF("customer_id", "dt", "nested")

      val expected = Seq(
        ("c1", Seq(("g1", 2), ("g2", 2))),
        ("c2", Seq(("g1", 2)))
      )
        .toDF("customer_id", "maxNested")

      val dataType = df.schema.find(_.name == "nested").get.dataType

      println(dataType)
      val ts = All()
      val feats = Seq(NestedMaxFeat("maxNested", Seq(ts), dataType, Seq("_1"), "_2", "nested", Seq(ts)))

      val actual = FeatExecutor(feats, df, new DateTime("2019-02-10")).execute()

      assertDataFrameEqualsUnordered(
        expected.select($"customer_id", explode($"maxNested")),
        actual.select($"customer_id", explode($"maxNested"))
      )
    }
  }

  describe("nested sum feat") {

    it("should create aggregate function that collects column as set and apply sumBy the specified column") {
      import spark.sqlContext.implicits._

      val d1 = Date.valueOf("2019-02-01")
      val d2 = Date.valueOf("2019-02-02")

      val df = Seq(
        ("c1", d1, Seq(("g1", 3L), ("g2", 1L))),
        ("c1", d2, Seq(("g1", 3L), ("g1", 2L), ("g2", 2L))),
        ("c2", d1, Seq(("g1", 1L))),
        ("c2", d2, Seq(("g1", 2L)))
      )
        .toDF("customer_id", "dt", "nested")

      val expected = Seq(
        ("c1", Seq(("g1", 8L), ("g2", 3L))),
        ("c2", Seq(("g1", 3L)))
      )
        .toDF("customer_id", "maxNested")

      val dataType = df.schema.find(_.name == "nested").get.dataType

      println(dataType)
      val ts = All()
      val feats = Seq(NestedSumByFeat("maxNested", Seq(ts), dataType, groupByCols = Seq("_1"), sumOverCols = Seq("_2"), "nested", Seq(ts)))

      val actual = FeatExecutor(feats, df, new DateTime("2019-02-10")).execute()

      assertDataFrameEqualsUnordered(
        expected.select($"customer_id", explode($"maxNested")),
        actual.select($"customer_id", explode($"maxNested"))
      )
    }

    it("should create aggregate function that collects column as set and apply sumBy on the specified columns") {
      import spark.sqlContext.implicits._

      val d1 = Date.valueOf("2019-02-01")
      val d2 = Date.valueOf("2019-02-02")

      val df = spark.sparkContext.parallelize(Seq(
        SomeFeature("c1", d1, Seq(NestedFeature("g1", "k1", 3L, -3), NestedFeature("g2", "k2", 1L, -1))),
        SomeFeature("c1", d2, Seq(NestedFeature("g1", "k1", 3L, -3), NestedFeature("g1", "k1", 2L, -2), NestedFeature("g2", "k2", 2L, -2))),
        SomeFeature("c2", d1, Seq(NestedFeature("g1", "k1", 1L, -1))),
        SomeFeature("c2", d2, Seq(NestedFeature("g1", "k1", 2L, -2)))
      ))
        .toDF("customer_id", "dt", "nested")

      val expected = Seq(
        ("c1", Seq(NestedFeature("g1", "k1", 8L, -8), NestedFeature("g2", "k2", 3L, -3))),
        ("c2", Seq(NestedFeature("g1", "k1", 3L, -3)))
      )
        .toDF("customer_id", "maxNested")

      val dataType = df.schema.find(_.name == "nested").get.dataType

      println(dataType)
      val ts = All()
      val feats = Seq(NestedSumByFeat("maxNested", Seq(ts), dataType, groupByCols = Seq("key", "key2"), sumOverCols = Seq("value", "value2"), "nested", Seq(ts)))

      val actual = FeatExecutor(feats, df, new DateTime("2019-02-10")).execute()

      assertDataFrameEqualsUnordered(
        expected.select($"customer_id", explode($"maxNested")),
        actual.select($"customer_id", explode($"maxNested"))
      )
    }
  }
}

case class SomeFeature(customer_id: String, dt: Date, nestedFeatures: Seq[NestedFeature])
case class NestedFeature(key: String, key2: String, value: Long, value2: Double)
