package com.paytm.map.features

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait CommonSparkSpecHelper extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>
  var ss: SparkSession = _
  var sc: SparkContext = _
  var sql: SQLContext = _

  override def beforeAll() = {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("tests")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.driver.allowMultipleContexts", "true")

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext
    sql = ss.sqlContext
    sc.setLogLevel("warn")
  }

  override def afterAll() = {
    sc.stop()
    ss.close()
    super.afterAll()
  }
}
