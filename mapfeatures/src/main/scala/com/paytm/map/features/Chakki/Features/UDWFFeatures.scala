package com.paytm.map.features.Chakki.Features

import com.paytm.map.features.Chakki.Features.TimeSeries.tsType
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.DataType

//********************************* UDTF Features ***********************************
sealed trait UDWFFeat extends Feat {
  //abstract members
  val partitionBy: String
  val orderBy: String

  // defined members
  val dependency = Seq(getBaseColumn, partitionBy, orderBy)

  override def getTsColName(ts: tsType): String = s"$varName${ts.suffix}_UDWF"

  def getTsUDAFColName(ts: tsType) = s"$varName${ts.suffix}"

  def getUdafCol(ts: tsType): Column = first(col(getTsColName(ts)), ignoreNulls = true).cast(dataType).as(getTsUDAFColName(ts))

  def getWindPartn: String = s"UDWF_$partitionBy"
}

case class GetFirstFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, partitionBy: String, orderBy: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDWFFeat {
  val outCol: Column = first(getBaseColumn, ignoreNulls = true).over(Window.partitionBy(partitionBy).orderBy(orderBy))
}

case class GetLastFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, partitionBy: String, orderBy: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDWFFeat {
  val outCol: Column = first(getBaseColumn, ignoreNulls = true).over(Window.partitionBy(partitionBy).orderBy(col(orderBy).desc))
}