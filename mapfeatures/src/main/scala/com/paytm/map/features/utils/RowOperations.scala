package com.paytm.map.features.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.util.Try

object RowOperations {

  implicit class RowExtentions(r: Row) {
    def getAs(key: String, dataType: DataType) = dataType match {
      case DataTypes.StringType  => r.getAs[String](key)
      case DataTypes.IntegerType => r.getAs[Int](key)
      case DataTypes.LongType    => r.getAs[Long](key)
      case DataTypes.DoubleType  => r.getAs[Double](key)
    }

    def getOrNull(key: String, dataType: DataType) = {
      Try(r.getAs(key, dataType)).toOption.orNull
    }
  }

}
