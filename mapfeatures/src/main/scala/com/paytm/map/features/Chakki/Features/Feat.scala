package com.paytm.map.features.Chakki.Features

import com.paytm.map.features.Chakki.Features.TimeSeries.tsType
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

// Base Trait for all feature definitions
trait Feat {
  // abstract members
  val varName: String // Column Name without TS information
  val timeAgg: Seq[tsType] // TimeSeries list for which the column is to be calculated
  val dataType: DataType // datatype of the column to be transformed to.
  val isFinal: Seq[tsType] // Timeseries for which the variable would be exported.
  val outCol: Column // Internal column to be implemented in all Features defining the core functionality
  val baseColumn: String //BaseColumns on which the function is calculated upon
  val dependency: Seq[String] // list of columns on which function is dependent on.

  // defined members
  /**
   * finalTs : If isFinal is not defined in that case
   * all the calculated columns are exposed in end dataset
   */
  val finalTs: Seq[tsType] = if (isFinal == null) timeAgg else isFinal

  /**
   * Functions applies the cast function and gets the column with outCol timeseries - suffixed
   *
   * @param ts : TimeSeries for which column is required.
   * @return : Column expression for the time series specified
   */
  def getTsCol(ts: tsType): Column = outCol.cast(dataType).as(getTsColName(ts))

  /**
   * Get the TimeSeries suffixed column name
   *
   * @param ts : Time Series for which info is to be suffixed
   * @return : Time Series information suffixed column name
   */
  def getTsColName(ts: tsType): String = s"$varName${ts.suffix}"

  /**
   * In Cases where baseColumn is not specified then the column name with `varName` is looked as baseColumn
   *
   * @return : baseColumn if not defined then varName
   */
  def getBaseColumn: String = if (baseColumn.isEmpty) varName else baseColumn
}