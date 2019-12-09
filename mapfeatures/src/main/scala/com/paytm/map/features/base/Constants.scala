package com.paytm.map.features.base

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, Encoders, Row}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types._

object Constants {
  val gaDelayOffset = 1 // GA reports are delayd in ingest by these many days
  val gaBackFillDays = 2 // By default backfill is done for 1 day, max it can work for is 5 days
  val productSpanOffset = 360 // How many days of data to take from CatalogProduct and FulfillmentRecharge

  val successfulTxnFlag: Column = when(col("status").isin(7), 1).otherwise(0)
  val successfulTxnFlagEcommerce = when(col("status").isin(0, 1, 21), 0).otherwise(1)

}
