package com.paytm.map.features.utils

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

object DataTypeExtensions {

  implicit class DataTypeHelper(dataType: DataType) {

    def sameType(other: DataType): Boolean = equalsIgnoreNullability(dataType, other)

    /**
     * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
     */
    def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
      (left, right) match {
        case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
          equalsIgnoreNullability(leftElementType, rightElementType)
        case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) =>
          equalsIgnoreNullability(leftKeyType, rightKeyType) &&
            equalsIgnoreNullability(leftValueType, rightValueType)
        case (StructType(leftFields), StructType(rightFields)) =>
          leftFields.length == rightFields.length &&
            leftFields.zip(rightFields).forall {
              case (l, r) =>
                l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
            }
        case (l, r) => l == r
      }
    }

  }
}
