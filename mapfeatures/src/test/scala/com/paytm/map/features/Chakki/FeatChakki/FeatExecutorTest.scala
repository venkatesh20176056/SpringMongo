package com.paytm.map.features.Chakki.FeatChakki

import com.paytm.map.features.Chakki.Features.{CollectUnikFeat, PostPaidTopMerchantFeat}
import com.paytm.map.features.Chakki.Features.TimeSeries.All
import com.paytm.map.features.utils.DataFrameTestBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode
import org.joda.time.format.DateTimeFormat

class FeatExecutorTest extends DataFrameTestBase {

  describe("CollectUnikFeat") {
    import spark.sqlContext.implicits._
    import com.paytm.map.features.base.DataTables.DFCommons

    val date = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime("2019-01-01")

    it("should collect unique longs from lists of longs and cast to array of strings") {

      val df = Seq(
        (1234L, Array(1L, 2L), "2019-01-01"),
        (1234L, Array(1L, 3L), "2019-01-01"),
        (2345L, Array(1L, 4L), "2019-01-01")
      )
        .toDF("customer_id", "baseCol", "dt")

      val feat = CollectUnikFeat("feat", Seq(All()), ArrayType(StringType), "baseCol")

      val actual = FeatExecutor(Seq(feat), df, date).execute()
      val expected = Seq(
        (1234L, Array(1L, 2L, 3L)),
        (2345L, Array(1L, 4L))
      )
        .toDF("customer_id", "feat")
        .alignSchema(actual.schema)

      assertDataFrameEqualsUnordered(actual, expected)
    }

    it("should create set of structs from lists of structs") {
      val one = MixedType(1, "a")
      val two = MixedType(2, "b")
      val three = MixedType(3, "c")
      val four = MixedType(4, "d")
      val sqlSchema = ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType)
      )))

      val df = Seq(
        (1234L, Array(one, two), "2019-01-01"),
        (1234L, Array(one, three), "2019-01-01"),
        (2345L, Array(one, four), "2019-01-01")
      )
        .toDF("customer_id", "baseCol", "dt")

      val feat = CollectUnikFeat("feat", Seq(All()), sqlSchema, "baseCol")

      val actual = FeatExecutor(Seq(feat), df, date).execute()
      val expected = Seq(
        (1234L, Array(one, two, three)),
        (2345L, Array(one, four))
      )
        .toDF("customer_id", "feat")
        .alignSchema(actual.schema)

      assertDataFrameEqualsUnordered(
        actual.withColumn("feat", explode($"feat")),
        expected.withColumn("feat", explode($"feat"))
      )
    }
  }

  describe("PostPaidTopMerchantFeat") {
    it("should return top merchant") {
      import spark.implicits._

      val date = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime("2019-01-01")

      val one = MixedTypeLong(10L, "m1")
      val two = MixedTypeLong(15L, "m2")

      val df = Seq(
        (123L, Array(one, two, one), "2019-01-01"),
        (456L, Array(one, two), "2019-01-01")
      ).toDF("customer_id", "nested", "dt")

      val func = PostPaidTopMerchantFeat("out", Seq(All()), StringType, Seq("b"), "a", "nested")

      val result = FeatExecutor(Seq(func), df, date).execute().orderBy("customer_id")

      val expected = Seq(
        (123L, "m1"),
        (456L, "m2")
      ).toDF("customer_id", "out")

      assertDataFrameEquals(expected, result)
    }
  }
}

case class MixedType(a: Int, b: String)

case class MixedTypeLong(a: Long, b: String)

