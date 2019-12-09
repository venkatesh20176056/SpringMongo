package com.paytm.map.features.sanity

import com.paytm.map.features.utils.Percent
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

case class TestFeat(testCaseName: String, testCaseCode: String, functionName: String, baseColumn: Seq[String], testTolerance: Percent = Percent.Zero)

object myUDFFunctionsX {

  // Example UDF users can write.
  def userUDF(x: Column, y: Column, z: Column): Column = x + y / z

  def getDayofTheweek(x: Column): Column = from_unixtime(unix_timestamp(x, "yyyy-MM-dd"), "E")

  def evaluateISIN(x: Column, y: String): Column = x.isin(y.split(","): _*)

  def evaluateISBetween(x: Column, y: String): Column = x.geq(y.split(",").head.asInstanceOf[Double]) and x.leq(y.split(",").last.asInstanceOf[Double])

  def evaluateSumLtEqOne(x: Seq[Column]): Column = round(x.reduce(_ + _), 2) <= lit(1)

}

import myUDFFunctionsX._

object ParseSanityFeatConfig {

  private def parseUDFFeatures(featList: Seq[TestFeat]) = {
    featList
      .map { feat =>
        val baseColumn = feat.baseColumn
        val feature = feat.functionName match {
          case "lt"          => col(baseColumn.head) < col(baseColumn(1))
          case "ltEq"        => col(baseColumn.head) <= col(baseColumn(1))
          case "Eq"          => col(baseColumn.head) === col(baseColumn(1))
          case "isIn"        => evaluateISIN(col(baseColumn.head), baseColumn(1))
          case "isNotNullVV" => (col(baseColumn.head).isNotNull and col(baseColumn(1)).isNotNull) or (col(baseColumn.head).isNull and col(baseColumn(1)).isNull)
          case "between"     => evaluateISBetween(col(baseColumn.head), baseColumn(1))
          case "sumLtEqOne"  => evaluateSumLtEqOne(baseColumn.map(col))
        }
        feature.as(feat.testCaseCode)
      }
  }

  private def generateTestDataSet(data: DataFrame, UDFCol: Seq[Column]) = {
    val newSetCol = data.columns.map(col) ++ UDFCol
    data.select(newSetCol: _*)
  }

  private def testEvaluatorMetrics(data: DataFrame, colName: String) = {
    val ll = (
      data.select(colName).where(col(colName) === true).count,
      data.select(colName).where(col(colName) === false).count,
      data.select("customer_id").where(col(colName) === false).limit(10).collect().map(_.getAs[Long]("customer_id").toString()).mkString("|")
    )
    ll
  }

  def executeFeaturesDebug(featList: Seq[TestFeat]): Unit = {

    val preUDFCol = parseUDFFeatures(featList)
    println(preUDFCol.size)
  }

  def executeFeatures(featList: Seq[TestFeat], data: DataFrame, targetDateStr: String) = {

    val preUDFCol = parseUDFFeatures(featList)
    print(preUDFCol.size)
    preUDFCol.foreach(println)
    val testDataSet = generateTestDataSet(data, preUDFCol)

    val testEvaluationMetrics = featList.map(feat =>
      (
        feat.testCaseCode,
        feat.testCaseName,
        feat.functionName,
        feat.baseColumn.mkString("|"),
        testEvaluatorMetrics(testDataSet, feat.testCaseCode),
        feat.testTolerance.getAsDouble
      ))

    (testDataSet, testEvaluationMetrics)
  }
}
