package com.paytm.map.features.Chakki.Features

import com.paytm.map.features.Chakki.FeatChakki._
import com.paytm.map.features.Chakki.Features.TimeSeries.tsType
import com.paytm.map.features.utils.UDFs.withPrefixes
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

//********************************* UDF Features ***********************************
sealed trait UDFFeat extends Feat {
  // Abstract members
  val stage: UDF_T
  override val outCol: Column = null

  //TODO: Please don't forget to add dependency here as the dependency here is used
  //TODO: for automatically including the dependent features in generation list

}

case class DivsnFeatUDF(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, downCol: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDFFeat {
  val dependency: Seq[String] = timeAgg.flatMap(ts => Seq(s"$getBaseColumn${ts.suffix}", s"$downCol${ts.suffix}"))
  override def getTsCol(ts: tsType): Column = (col(s"$getBaseColumn${ts.suffix}") / col(s"$downCol${ts.suffix}")).cast(dataType).as(getTsColName(ts))
}

case class Pack2MapFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, pattern: String, catList: Seq[(String, String)], isFinal: Seq[tsType] = null) extends UDFFeat {
  val baseColumn = ""
  val dependency: Seq[String] = timeAgg.flatMap(ts => catList.map { case (merch, cat) => s"${merch}_${cat}_$pattern${ts.suffix}" })

  override def getTsCol(ts: tsType): Column = {
    val getTupleLitCol = udf((x: String, y: String) => (x, y))
    val baseColList = catList
      .map { case (merch, cat) => (merch, cat, s"${merch}_${cat}_$pattern${ts.suffix}") }
      .map {
        case (merch, cat, colM) => struct(
          lit(merch).as("merchant"),
          lit(cat).as("category"),
          col(colM).as("value")
        )
      }
    array(baseColList: _*).cast(dataType).as(getTsColName(ts))
  }
}

case class ListSizeFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, baseColumn: String, isFinal: Seq[tsType] = null) extends UDFFeat {
  val dependency: Seq[String] = timeAgg.map(ts => s"$baseColumn${ts.suffix}")

  override def getTsCol(ts: tsType): Column = size(col(s"$baseColumn${ts.suffix}")).cast(dataType).as(getTsColName(ts))

}

case class AddFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, baseColumn: String, isFinal: Seq[tsType] = null) extends UDFFeat {

  private val baseColumns = baseColumn.split(",").map(_.trim)
  val dependency: Seq[String] = timeAgg.flatMap(ts => baseColumns.map(colm => s"$colm${ts.suffix}"))

  override def getTsCol(ts: tsType): Column = baseColumns
    .map(colm => coalesce(col(s"$colm${ts.suffix}"), lit(0)))
    .reduce(_ + _)
    .cast(dataType)
    .as(getTsColName(ts))
}

case class GetPrefByFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, isMax: Int, pattern: String, prefixList: Seq[String], isFinal: Seq[tsType] = null) extends UDFFeat {
  val baseColumn = ""
  val dependency: Seq[String] = timeAgg.flatMap(ts => prefixList.map { prefix => s"$prefix$pattern${ts.suffix}" })

  override def getTsCol(ts: tsType): Column = {
    val baseColList = prefixList
      .map(prefix => (s"$prefix$pattern${ts.suffix}", prefix))
      .map {
        case (colM, prefix) =>
          when(col(colM).isNotNull, struct(col(colM).as("v"), lit(prefix).as("k"))).otherwise(null)
      }

    val prefCol = (if (isMax == 1) greatest(baseColList: _*)
    else least(baseColList: _*)).getItem("k")

    prefCol.cast(dataType).as(getTsColName(ts))
  }
}

case class WeekDayFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, baseColumn: String, isFinal: Seq[tsType] = null) extends UDFFeat {
  val dependency: Seq[String] = timeAgg.map(ts => s"$baseColumn${ts.suffix}")

  override def getTsCol(ts: tsType): Column = from_unixtime(unix_timestamp(col(s"$baseColumn${ts.suffix}"), "yyyy-MM-dd"), "E").cast(dataType).as(getTsColName(ts))
}

case class GeohashPrefixFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, stage: UDF_T, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDFFeat {
  val dependency: Seq[String] = timeAgg.map(ts => s"$baseColumn${ts.suffix}")

  override def getTsCol(ts: tsType): Column = udf(withPrefixes)
    .apply(col(s"$getBaseColumn${ts.suffix}"))
    .cast(dataType)
    .as(getTsColName(ts))
}