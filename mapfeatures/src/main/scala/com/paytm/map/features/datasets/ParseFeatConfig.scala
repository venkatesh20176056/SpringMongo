package com.paytm.map.features.datasets

import com.paytm.map.features.utils.ArgsUtils
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object myUDFFunctions {

  case class Feat(
    name: String,
    functionType: String,
    functionName: String,
    timeAgg: Seq[Int],
    baseColumn: Seq[String],
    dataType: DataType,
    isFinal: Seq[Int] = Seq[Int]()
  )

  def getFirstLastWindow(partitionBy: String, orderBy: String, isFirst: Boolean = true): WindowSpec = {
    val window = Window.partitionBy(partitionBy)
    if (isFirst)
      window.orderBy(orderBy)
    else
      window.orderBy(col(orderBy).desc)
  }

  val collectUnique: UserDefinedFunction = udf[Seq[String], Seq[Seq[String]]]((seqOfSeqX: Seq[Seq[String]]) => seqOfSeqX.flatten.distinct)

  // Example UDF users can write.
  def userUDF(x: Column, y: Column, z: Column): Column = x + y / z

  def getDayofTheweek(x: Column): Column = from_unixtime(unix_timestamp(x, "yyyy-MM-dd"), "E")

}

object ParseFeatConfig {

  import myUDFFunctions._

  private def printFeatures(feats: Any): Unit = {
    feats match {
      case colList: Seq[Column] =>
        println(colList.size)
        colList.foreach(x => println(s"\t$x"))

      case colObj: Map[String, (Seq[Column], Seq[Column], Seq[String])] =>
        println(colObj.size)
        colObj.foreach { x =>
          println(s"\t${x._1}")
          x._2._1.foreach(newX => println(s"\t\t$newX"))
        }

      case everyObj => println(everyObj)

    }
  }

  private def parseUDFFeatures(stage: String, timeAgg: Int, featList: Seq[Feat], prefix: String = ""): Seq[Column] = {
    featList
      .filter(_.functionType == stage)
      .filter(_.timeAgg.contains(timeAgg))
      .map { feat =>
        val baseColumn = feat.baseColumn.map(colM => col(colM + prefix))
        val feature = feat.functionName match {
          case "avg"          => baseColumn.head / baseColumn(1)
          case "userUDF"      => userUDF(baseColumn.head, baseColumn(1), baseColumn(2))
          case "DayOfTheWeek" => getDayofTheweek(baseColumn.head)
        }
        val featName = feat.name + prefix
        val dataType = feat.dataType
        feature.cast(dataType).as(featName)
      }
  }

  private def parseUDAFFeatures(timeAgg: Int, featList: Seq[Feat], prefix: String = ""): Seq[Column] = {
    featList
      .filter(_.functionType == "UDAF")
      .filter(_.timeAgg.contains(timeAgg))
      .map { feat =>
        val baseColumn = col(feat.baseColumn.head)
        val feature = feat.functionName match {
          case "sum"             => sum(baseColumn)
          case "min"             => min(baseColumn)
          case "max"             => max(baseColumn)
          case "avg"             => avg(baseColumn)
          case "count"           => count(baseColumn)
          case "collect_as_list" => collect_list(baseColumn)
          case "collect_as_set"  => collect_set(baseColumn)
          case "count_distinct"  => approx_count_distinct(baseColumn)
          case "collect_unique"  => collectUnique(collect_set(baseColumn))
        }
        val featName = feat.name + prefix
        val dataType = feat.dataType
        feature.cast(dataType).as(featName)
      }
  }

  private def parseUDWFFeatures(timeAgg: Int, featList: Seq[Feat], prefix: String = ""): Map[String, (Seq[Column], Seq[Column], Seq[String])] = {
    featList
      .filter(_.functionType == "UDWF")
      .filter(_.timeAgg.contains(timeAgg))
      .map { feat =>
        val featName = feat.name + prefix
        val featNameUDWF = featName + "_UDWF"
        val dataType = feat.dataType
        val baseColumn = feat.baseColumn
        feat.functionName match {
          case "getFirst" => ("UDWF" + "_" + baseColumn(1), first(baseColumn.head, ignoreNulls = true).over(getFirstLastWindow(baseColumn(1), baseColumn(2))).cast(dataType).as(featNameUDWF), first(featNameUDWF).cast(dataType).as(featName), baseColumn)
          case "getLast"  => ("UDWF" + "_" + baseColumn(1), first(baseColumn.head, ignoreNulls = true).over(getFirstLastWindow(baseColumn(1), baseColumn(2), isFirst = false)).cast(dataType).as(featNameUDWF), first(featNameUDWF).cast(dataType).as(featName), baseColumn)
        }
      }.groupBy(_._1)
      .map(kv => (kv._1, (kv._2.map(_._2), kv._2.map(_._3), kv._2.flatMap(_._4).distinct)))
  }

  private def getTimeSeriesData(data: DataFrame, targetDate: String, days: Int): Dataset[Row] = {
    val startDate = ArgsUtils.formatter.parseDateTime(targetDate).minusDays(days).toString(ArgsUtils.formatter)
    data
      .where(col("dt") > startDate)
      .where(col("dt") <= targetDate)

  }

  private def UDAFFuncDispatcher(data: DataFrame, UDAFCol: Seq[Column], baseCols: Seq[String] = Seq("*")): DataFrame = {
    data
      .select(baseCols.head, baseCols.tail: _*)
      .groupBy("customer_id")
      .agg(UDAFCol.head, UDAFCol.tail: _*)
  }

  private def UDFFuncDispatcher(data: DataFrame, UDFCol: Seq[Column]): DataFrame = {
    val newSetCol = data.columns.map(col) ++ UDFCol
    data.select(newSetCol: _*)
  }

  private def UDWFFuncDispatcher(data: DataFrame, UDWFCol: Seq[Column], UDWAFCol: Seq[Column], baseColumn: Seq[String]): DataFrame = {

    val allCol = baseColumn.map(col) ++ UDWFCol

    data
      .select(baseColumn.head, baseColumn.tail: _*)
      .select(allCol: _*)
      .groupBy("customer_id")
      .agg(UDWAFCol.head, UDWAFCol.tail: _*)
  }

  private def getTimeSeriesMaterialized(data: DataFrame, featList: Seq[Feat], timeAgg: Int, prefix: String = ""): DataFrame = {

    //Parse Featureset
    val udafCol = parseUDAFFeatures(timeAgg, featList, prefix)
    val udwfColSets: Map[String, (Seq[Column], Seq[Column], Seq[String])] = parseUDWFFeatures(timeAgg, featList, prefix).filter(_._2._1.nonEmpty)
    val udfCol = parseUDFFeatures("POST-UDF", timeAgg, featList, prefix)

    //Print Generated Columns For Quick debugging
    println(s"Printing the UDAF Columns for timeSeries=$timeAgg")
    printFeatures(udafCol)

    println(s"Printing the UDWF Columns for timeSeries=$timeAgg")
    printFeatures(udwfColSets)

    println(s"Printing the POST UDF Columns for timeSeries=$timeAgg")
    printFeatures(udfCol)

    //execute UDAF
    val udafBaseCol = (Seq("customer_id", "dt") ++
      featList.filter(_.functionType == "UDAF")
      .filter(_.timeAgg.contains(timeAgg)).flatMap(_.baseColumn)).distinct

    val udafData = if (udafCol.nonEmpty) {
      UDAFFuncDispatcher(data, udafCol, udafBaseCol)
    } else data

    //execute UDWF
    val udwfData = udwfColSets.map {
      case (_: String, (udwfCol: Seq[Column], udwafCol: Seq[Column], baseColumn: Seq[String])) =>
        UDWFFuncDispatcher(data, udwfCol, udwafCol, baseColumn)
    }.toSeq

    val tsFeatData = (udafCol.nonEmpty, udwfColSets.nonEmpty, udfCol.nonEmpty) match {
      // Handling of cases when one of the types of feature is not requested
      case (true, true, true)   => UDFFuncDispatcher((udafData +: udwfData).joinAll(Seq("customer_id"), "outer"), udfCol)
      case (true, true, false)  => (udafData +: udwfData).joinAll(Seq("customer_id"), "outer")

      case (true, false, true)  => UDFFuncDispatcher(udafData, udfCol)
      case (true, false, false) => udafData

      case (false, true, true)  => UDFFuncDispatcher(udwfData.joinAll(Seq("customer_id"), "outer"), udfCol)
      case (false, true, false) => udwfData.joinAll(Seq("customer_id"), "outer")

      // case (false,false,true) => getUDFMaterialized(udafData,udfCol) // Note : Not Possible as udfCol like average required aggregates in stage UDAF
      // case (false,false,false) => udafData // Note: will be filtered in the last distinct timeseries stage
    }

    // Only select final output Columns.
    val tsFinalFeatureList = "customer_id" +: getFinalColumns(timeAgg, featList, prefix)
    tsFeatData.select(tsFinalFeatureList.head, tsFinalFeatureList.tail: _*)
  }

  private def getFinalColumns(timeAgg: Int, featList: Seq[Feat], prefix: String = "") = {
    featList
      .filter(_.timeAgg.contains(timeAgg))
      .filter(_.functionType != "PRE-UDF") // As pre-udf features can't make it to finalset
      .filter(_.isFinal.contains(timeAgg))
      .map(_.name + prefix)
  }

  def executeFeatures(featList: Seq[Feat], data: DataFrame, targetDateStr: String): DataFrame = {

    //make PRE UDF Features
    val preUDFCol = parseUDFFeatures("PRE-UDF", -1, featList)
    println("Printing the PRE UDF Columns")
    printFeatures(preUDFCol)
    val preUDFData = UDFFuncDispatcher(data, preUDFCol)

    // Now Run the timeSeries Partitions
    val unqTimeSeries = featList
      .filter(_.functionType != "PRE-UDF")
      .flatMap(_.timeAgg)
      .distinct

    unqTimeSeries.map {
      case -1 => getTimeSeriesMaterialized(preUDFData, featList, -1)
      case ts: Int =>
        val tsData = getTimeSeriesData(preUDFData, targetDateStr, ts)
        getTimeSeriesMaterialized(tsData, featList, ts, s"_${ts}_days")
    }.joinAll(Seq("customer_id"), "outer")
  }
}
