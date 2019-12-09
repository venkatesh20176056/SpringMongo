package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.utils.DataFrameTestBase
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

class RelativeRecallMetricSpec extends DataFrameTestBase {

  import spark.implicits._

  describe("relative recall generation") {

    it("should match relative recall") {

      val oldRes = Seq(("1", 1.0), ("11", 1.0), ("4", 1.0), ("2", 1.0), ("9", 1.0), ("13", 1.0), ("15", 1.0), ("7", 1.0), ("5", 1.0), ("25", 1.0)).toDF("customer_id", "score")
      val newRes = Seq(("7", 10.0), ("5", 9.0), ("2", 8.0), ("1", 7.0), ("4", 6.0), ("15", 5.0), ("11", 4.0), ("13", 3.0), ("9", 2.0), ("25", 1.0)).toDF("customer_id", "score")
      val truth = Seq("7", "5", "1", "15", "13").toDF("customer_id")
      val res = Seq(
        Row(0, 0L, 0L, 0.0, 5L),
        Row(0, 0L, 0L, 0.0, 5L),
        Row(0, 0L, 0L, 0.0, 5L),
        Row(0, 0L, 0L, 0.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(1, 1L, 1L, 1.0, 5L),
        Row(2, 1L, 2L, 2.0, 5L),
        Row(2, 1L, 2L, 2.0, 5L),
        Row(2, 1L, 2L, 2.0, 5L),
        Row(2, 1L, 2L, 2.0, 5L),
        Row(3, 1L, 2L, 2.0, 5L),
        Row(3, 1L, 2L, 2.0, 5L),
        Row(3, 1L, 2L, 2.0, 5L),
        Row(3, 1L, 2L, 2.0, 5L),
        Row(3, 1L, 2L, 2.0, 5L),
        Row(4, 1L, 3L, 3.0, 5L),
        Row(4, 1L, 3L, 3.0, 5L),
        Row(5, 1L, 3L, 3.0, 5L),
        Row(5, 1L, 3L, 3.0, 5L),
        Row(6, 2L, 4L, 2.0, 5L),
        Row(6, 2L, 4L, 2.0, 5L),
        Row(7, 3L, 4L, 4.0 / 3, 5L),
        Row(7, 3L, 4L, 4.0 / 3, 5L),
        Row(9, 5L, 5L, 1.0, 5L),
        Row(9, 5L, 5L, 1.0, 5L)
      )

      val struct = StructType(
        StructField("N", IntegerType, false) ::
          StructField("old_recall_count", LongType, false) ::
          StructField("new_recall_count", LongType, false) ::
          StructField("ratio", DoubleType, false) ::
          StructField("totalTrueCount", LongType, false) :: Nil
      )
      val resDF = spark.createDataFrame(spark.sparkContext.parallelize(res), struct)
      val recall = RelativeRecallMetric.recall(spark, oldRes, newRes, truth)
      recall.show(50)
      assertDataFrameEquals(recall, resDF)
    }
  }
}
