package com.paytm.map.features.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getCoalesceCount(spark: SparkSession): Integer = {
    val executorCount: Integer = spark.sparkContext.statusTracker.getExecutorInfos.length
    val coresPerExecutor: Integer =
      spark.sparkContext.range(0, 1).map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head
    val coalesceCount: Integer = coresPerExecutor * executorCount
    coalesceCount
  }
}
