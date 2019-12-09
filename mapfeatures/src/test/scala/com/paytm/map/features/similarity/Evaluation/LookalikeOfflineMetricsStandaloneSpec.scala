package com.paytm.map.features.similarity.Evaluation

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.paytm.map.features.utils.ArgsUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class LookalikeOfflineMetricsStandaloneSpec extends FlatSpec with Matchers with DataFrameSuiteBase {

  import spark.implicits._

  val startDate = DateTime.parse("2019-09-18")
  val startDateStr = startDate.toString(ArgsUtils.formatter)

  it should ("avgTraxCountLast7Days equals 4") in {

    val baseData: DataFrame = Seq(
      ("1", 1, "2019-08-08"),
      ("2", 3, "2019-02-08"),
      ("3", 5, "2019-06-08"),
      ("4", 7, "2019-04-08")
    ).toDF("customer_id", "PAYTM_total_transaction_count_7_days", "latest_open_date")

    val cids = Seq(
      "1", "4"
    ).toDF("cid")

    val results = 4d
    LookalikeOfflineMetricsStandalone.avgTraxCountLast7Days(spark, baseData, cids) should equal(results)
  }

  it should ("loginEvent in last 30 days equals 0.5") in {

    val baseData: DataFrame = Seq(
      ("1", 1, "2019-08-08"),
      ("2", 3, "2019-02-08"),
      ("3", 5, "2019-06-08"),
      ("4", 7, "2019-09-08")
    ).toDF("customer_id", "PAYTM_total_transaction_count_7_days", "latest_open_date")

    val cids = Seq(
      "2", "4"
    ).toDF("cid")

    val results = .5d
    LookalikeOfflineMetricsStandalone.loginEvent(spark, baseData, startDate, startDateStr, cids, cids.count()) should equal(results)
  }

  it should ("promotion code usage") in {

    val struct = StructType(
      Seq(
        StructField("customer_id", LongType, false),
        StructField(
          "PAYTM_promo_usage_dates",
          ArrayType(StructType(
            Seq(
              StructField("Promocode", StringType, true),
              StructField("Date", TimestampType, true)
            )
          )), true
        )
      )
    )

    val baseData = Seq(
      Row(1L, Array(Row("FREE1", Timestamp.valueOf("2019-09-07 00:00:00")), Row("FREE2", Timestamp.valueOf("2019-09-18 00:00:00")))),
      Row(2L, Array(Row("FREE6", Timestamp.valueOf("2019-08-12 00:00:00")), Row("FREE22", Timestamp.valueOf("2019-05-08 00:00:00")))),
      Row(3L, Array(Row("FREE3", Timestamp.valueOf("2019-07-09 00:00:00")), Row("FREE9", Timestamp.valueOf("2019-06-28 00:00:00")))),
      Row(4L, Array(Row("FREE4", Timestamp.valueOf("2019-06-06 00:00:00")), Row("FREE5", Timestamp.valueOf("2019-09-22 00:00:00")))),
      Row(5L, Array())
    )

    val cids = Seq(
      2L, 4L, 5L
    ).toDF("cid")

    val promo = spark.createDataFrame(spark.sparkContext.parallelize(baseData), struct)

    val results = (2 / 3d, 1 / 3d, 1 / 3d)

    LookalikeOfflineMetricsStandalone.promocodeUsage(spark, promo, startDate, startDateStr, cids, cids.count()) should equal(results)
  }
}
