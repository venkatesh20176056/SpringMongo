package com.paytm.map.features.similarity.Evaluation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class LookalikeOfflineCtrMetricsSpec extends FlatSpec with Matchers with DataFrameSuiteBase {

  import spark.implicits._

  it should ("bannerCTR should have 3 rows") in {

    val baseUnivese = Seq(
      (1, "2019-09-09", 0, 2, 2, 1, "app", "xx"),
      (2, "2019-08-09", 1, 3, 4, 1, "app", "xx"),
      (3, "2019-07-09", 1, 5, 6, 1, "app", "xx"),
      (4, "2019-05-09", 0, 7, 8, 1, "app", "xx"),
      (5, "2019-06-09", 1, 9, 10, 1, "app", "xx"),
      (6, "2019-04-09", 0, 11, 12, 1, "app", "xx"),
      (7, "2019-04-09", 1, 11, 12, 1, "app", "xx")
    ).toDF("customer_id", "date", "click", "slot", "banner_id", "impression", "channel", "carousel_name")

    val cids = Seq(
      1, 5, 6, 7
    ).toDF("customer_id")

    val struct = StructType(
      StructField("slot", IntegerType, false) ::
        StructField("CTR", DoubleType, true) ::
        StructField("customer_id_count", LongType, false) :: Nil
    )

    val results = Seq(
      Row(9, 1.0, 1L),
      Row(11, 0.5, 2L),
      Row(2, 0.0, 1L)
    )

    val ctr = spark.createDataFrame(spark.sparkContext.parallelize(results), struct)

    assertDataFrameEquals(LookalikeOfflineCtrMetrics.bannerCTR(spark, baseUnivese, cids), ctr)
  }
}
