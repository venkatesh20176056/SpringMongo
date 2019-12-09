package com.paytm.map.features.Chakki.FeatChakki

import com.paytm.map.features.Chakki.Features.TimeSeries.tsType
import com.paytm.map.features.Chakki.Features._

object FeatParser {
  def apply(ts: tsType, featList: Seq[Feat], entityId: String = "customer_id"): FeatParser = new FeatParser(ts, featList, entityId)
}

/**
 * Class Parses the featurelist and lets the parsed fgeatures access as attributed for a give tiome series
 *
 * @param ts       : TimeSeries object
 * @param featList : List of Feature Specification Object
 */
class FeatParser(ts: tsType, featList: Seq[Feat], entityId: String) {

  /**
   * FeatList filtered on timeseries
   */
  private val tsFeatList: Seq[Feat] = featList.filter(_.timeAgg.contains(ts))

  /**
   * Parse UDF Features as specified by stage
   *
   * @param stage : POST/PRE stages of UDF functions
   * @return : UDFParsed object with list of functions and dependencies
   */
  private def parseUDFFeatures(stage: UDF_T): UDFParsed = {
    val relevantFeat = tsFeatList
      .filter(_.isInstanceOf[UDFFeat])
      .filter(_.asInstanceOf[UDFFeat].stage == stage)

    val feats = relevantFeat.map { feat => feat.getTsCol(ts) }
    val dependency = relevantFeat.flatMap(_.dependency).distinct

    UDFParsed(feats, dependency)
  }

  val PREUDFFeatures: UDFParsed = parseUDFFeatures(PRE_UDF)
  val POSTUDFFeatures: UDFParsed = parseUDFFeatures(POST_UDF)

  /**
   * UDAFParsed Object with list of features and dependencies
   */
  val UDAFFeatures: UDAFParsed = {
    val udafFeat = tsFeatList
      .filter(_.isInstanceOf[UDAFFeat])
      .map(feat => (feat.getTsCol(ts), feat.dependency))
    val feats = udafFeat.map(_._1)
    val dependency = (udafFeat.flatMap(_._2) ++ Seq(entityId, "dt")).distinct
    UDAFParsed(feats, dependency)
  }

  /**
   * Window Functions object containing Map of partition with object
   * Each object specifies the window funcition needs to be applied
   * And Aggregations following the window function.
   */
  val UDWFFeatures: Map[String, UDWFParsed] = {
    tsFeatList
      .filter(_.isInstanceOf[UDWFFeat])
      .map(_.asInstanceOf[UDWFFeat])
      .map { udwfFeat =>
        (udwfFeat.getWindPartn, udwfFeat.getTsCol(ts), udwfFeat.getUdafCol(ts), udwfFeat.dependency)
      }.groupBy(_._1)
      .map(kv => (kv._1, UDWFParsed(kv._2.map(_._2), kv._2.map(_._3), kv._2.flatMap(_._4).distinct)))
  }

  /**
   * List of data dependencies for PREUDF,UDAF,UDWF functions.
   */
  val dataDependency: Seq[String] = (
    PREUDFFeatures.dependency
    ++ UDAFFeatures.dependency
    ++ UDWFFeatures.flatMap(_._2.dependency)
  ).distinct

  val FinalColumns: Seq[String] = {
    tsFeatList
      .filter(feat => !(feat.isInstanceOf[UDFFeat] && feat.asInstanceOf[UDFFeat].stage == PRE_UDF)) // As pre-udf features can't make it to finalset
      .filter(_.finalTs.contains(ts))
      .map(_.varName + ts.suffix)
  }

  /**
   * Print the featurs as speified by
   *
   * @param parsedFeat : Parsed Feature object
   */
  def printFeat(parsedFeat: ParsedFeat): Unit = {
    println(parsedFeat.feats.size)
    parsedFeat.feats.foreach(x => println(s"\t$x"))
  }

  /**
   * Print all features
   */
  def logAll(): Unit = {
    //Print Generated Columns For Quick debugging
    println(s"Printing the UDAF Columns for timeSeries=$ts")
    printFeat(UDAFFeatures)

    println(s"Printing the UDWF Columns for timeSeries=$ts")
    UDWFFeatures.foreach(x => printFeat(x._2))

    println(s"Printing the POST UDF Columns for timeSeries=$ts")
    printFeat(POSTUDFFeatures)
  }

}
