package com.paytm.map.features.Chakki.Features

import com.paytm.map.features.utils.ArgsUtils.FormatPattern
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime

object TimeSeries {

  val dtCol: String = "dt"

  // time series type
  sealed trait tsType {
    val suffix: String

    /**
     * Return the filter conditions for a given timeseries
     *
     * @param targetDate : filter condition is made basis the target execution stage
     * @return : boolean column with filter condition for a timeseries.
     */
    def filterCondn(targetDate: DateTime): Column

    /**
     * Give a colname and time series remove the suffix
     *
     * @param colName : Colname for which suffix is to be removed
     * @return : suffix stripped colname
     */
    def extractCol(colName: String): String = colName.stripSuffix(suffix)
  }

  //tsType
  case class All(suffix: String = "") extends tsType {
    def filterCondn(targetDate: DateTime): Column = col(dtCol) <= targetDate.toString(FormatPattern)

    override def toString: String = "All"
  }

  case class nMonths(n: Int = 0, suffix: String = "_this_month") extends tsType {
    def filterCondn(targetDate: DateTime): Column = {
      val lastMonth = targetDate.minusMonths(n)
      val startDate = lastMonth.dayOfMonth().withMinimumValue().toString(FormatPattern)
      val endDate = lastMonth.dayOfMonth().withMaximumValue().toString(FormatPattern)
      col(dtCol).between(startDate, endDate)
    }

    override def toString: String = s"M$n"
  }

  case class nMonthsInc(n: Int = 1) extends tsType {
    def filterCondn(targetDate: DateTime): Column = {
      val lastMonth = targetDate.minusMonths(n - 1)
      val startDate = lastMonth.dayOfMonth().withMinimumValue().toString(FormatPattern)
      val endDate = targetDate.toString(FormatPattern)
      col(dtCol).between(startDate, endDate)
    }

    val suffix: String = s"_${n}_calendar_months"

    override def toString: String = s"C$n"
  }

  case class nDays(days: Int) extends tsType {
    val suffix: String = s"_${days}_days"

    def filterCondn(targetDate: DateTime): Column = {
      val startDate = targetDate.minusDays(days - 1).toString(FormatPattern) //D-1 to avoid n+1 days in between
      val endDate = targetDate.toString(FormatPattern)
      col(dtCol).between(startDate, endDate)
    }

    override def toString: String = s"D$days"
  }

  // Time Series Functions
  val timeSeries: Map[String, tsType] = Map(
    ("All", All()),
    ("D1", nDays(1)),
    ("D3", nDays(3)),
    ("D7", nDays(7)),
    ("D15", nDays(15)),
    ("D30", nDays(30)),
    ("D45", nDays(45)),
    ("D60", nDays(60)),
    ("D90", nDays(90)),
    ("D180", nDays(180)),
    ("M0", nMonths()),
    ("M1", nMonths(1, "_last_month")),
    ("M2", nMonths(2, "_second_last_month")),
    ("M3", nMonths(3, "_third_last_month")),
    ("M4", nMonths(4, "_fourth_last_month")),
    ("M5", nMonths(5, "_fifth_last_month")),
    ("M6", nMonths(6, "_sixth_last_month")),
    ("C1", nMonthsInc(1)),
    ("C3", nMonthsInc(3)),
    ("C6", nMonthsInc(6)),
    ("C12", nMonthsInc(12))
  )

  // resolve suffix to ts
  val tsSuffixMap: Map[String, String] = timeSeries.filter(_._1 != "All").map(x => (x._2.suffix, x._1))

  /**
   * Resolve ts from entire columns name
   *
   * @param colName : ColName with ts suffix
   * @return : index of timeseries.
   */
  def getTimeSeries(colName: String): String = {
    val ts = tsSuffixMap
      .filter(x => colName.endsWith(x._1))
      .values.toSeq
      .sortBy(suffix => suffix.length)(Ordering[Int].reverse)
      .headOption
    if (ts.nonEmpty) ts.get else "All"
  }

  /**
   * From timeseries string extract parent col
   *
   * @param colName : TimeSeries Information suffixed column name
   * @return : ts suffix striped column name
   */
  def getParent(colName: String): String = timeSeries(getTimeSeries(colName)).extractCol(colName)
}
