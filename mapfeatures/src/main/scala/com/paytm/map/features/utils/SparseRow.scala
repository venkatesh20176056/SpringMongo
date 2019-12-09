package com.paytm.map.features.utils

import breeze.collection.mutable.SparseArray
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SparseRow {

  def create(types: StructType, values: Any*): Row = {
    new SparseRow(types, values.toArray)
  }

}

object NoValue {}

class SparseRow() extends Row {

  var arr: SparseArray[Any] = _
  var types: StructType = _

  def this(types: StructType, values: Array[Any]) = {
    this()
    this.types = types
    arr = new SparseArray[Any](values.length, NoValue)
    values.zip(types).zipWithIndex.foreach(e => {
      val noVal = null //getNoValueForType(e._1._2)
      if (e._1._1 != noVal)
        arr.update(e._2, e._1._1)
    })
  }

  private def this(types: StructType, arr: SparseArray[Any]) = {
    this()
    this.types = types
    this.arr = arr
  }

  private def getNoValueForType(t: StructField): Any = {

    if (t.dataType.equals(DataTypes.DoubleType)) 0.0d
    else if (t.dataType.equals(DataTypes.FloatType)) 0.0f
    else if (t.dataType.equals(DataTypes.IntegerType)) 0
    else if (t.dataType.equals(DataTypes.LongType)) 0l
    else if (t.dataType.equals(DataTypes.BooleanType)) false
    else if (t.dataType.equals(DataTypes.ShortType)) 0.asInstanceOf[Number].shortValue()
    else null

  }

  override def length: Int = arr.length

  override def get(i: Int): Any = {
    var res = arr.apply(i)
    if (res == NoValue)
      res = null //getNoValueForType(types.fields(i))
    res
  }

  override def copy(): Row = new SparseRow(this.types, this.arr)
}