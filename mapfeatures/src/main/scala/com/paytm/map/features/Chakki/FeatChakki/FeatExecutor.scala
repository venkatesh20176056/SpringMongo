package com.paytm.map.features.Chakki.FeatChakki

import com.paytm.map.features.Chakki.Features.TimeSeries._
import com.paytm.map.features.Chakki.Features._
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object FeatExecutor {
  def apply(featList: Seq[Feat], data: DataFrame, targetDate: DateTime, entityId: String = "customer_id"): FeatExecutor = new FeatExecutor(featList, data, targetDate, entityId)
}

/**
 * Class Implements the flow of execution for a givene features specifications
 *
 * @param featList   : Feature specification object list
 * @param data       :  Data on which the features specifications to be applied.
 * @param targetDate : TargetDate for executioon or timeseries.
 */
class FeatExecutor(featList: Seq[Feat], data: DataFrame, targetDate: DateTime, entityId: String) {

  var isLog = true
  private lazy val preUDFData: DataFrame = {
    //make PRE UDF Features
    val parsedFeat = FeatParser(All(), featList, entityId)
    val preUDFCol = parsedFeat.PREUDFFeatures

    if (isLog) {
      println("Printing the PRE UDF Columns")
      parsedFeat.printFeat(preUDFCol)
    }

    UDFFuncDispatcher(data, preUDFCol)
  }
  private lazy val uniqTimeSeries = {
    // Now Run the timeSeries Partitions
    featList
      .filter(feat => !(feat.isInstanceOf[UDFFeat] && feat.asInstanceOf[UDFFeat].stage == PRE_UDF))
      .flatMap(_.timeAgg)
      .distinct
  }

  /**
   * Execute the feature specification for all time series
   *
   * @return : DataFrame with specified columns
   */
  def execute(): DataFrame = {
    // Run for Time Series Partitions
    uniqTimeSeries.map(ts => execute(ts)).joinAllWithoutRepartiton(Seq(entityId), "outer")
  }

  /**
   * Execute the feature specification for a given time series
   *
   * @param ts : Time Series Object.
   * @return :  DataFrame with specified columns of a timeseries ts
   */
  def execute(ts: tsType): DataFrame = {
    val tsData = getTimeSeriesData(preUDFData, ts)
    getTimeSeriesMaterialized(tsData, FeatParser(ts, featList, entityId))
  }

  /**
   * Apply time series filters
   *
   * @param preUDFData : Pre udf functions applied data
   * @param ts         : time series object
   * @return : filtered data for ts specification
   */
  private def getTimeSeriesData(preUDFData: DataFrame, ts: tsType): Dataset[Row] = {
    preUDFData.where(ts.filterCondn(targetDate))
  }

  /**
   * Function Applies the ts specification on the preudf functions applied data
   *
   * @param preUDFData : PRE UDF transformed data
   * @param parsedFeat : set of parsed features for a time series
   * @return : DataFrame object containing columns as specified by parsed features.
   */
  private def getTimeSeriesMaterialized(preUDFData: DataFrame, parsedFeat: FeatParser): DataFrame = {

    //Parse Featureset
    val udafFeat = parsedFeat.UDAFFeatures
    val udwfFeatSet = parsedFeat.UDWFFeatures.filter(_._2.feats.nonEmpty)
    val udfFeat = parsedFeat.POSTUDFFeatures

    // Log features for Debugging
    if (isLog) parsedFeat.logAll()

    //execute UDAF
    val udafData = if (udafFeat.feats.nonEmpty) {
      UDAFFuncDispatcher(preUDFData, udafFeat)
    } else preUDFData

    //execute UDWF
    val udwfData = udwfFeatSet.map {
      case (_: String, udwfFeat: UDWFParsed) =>
        UDWFFuncDispatcher(preUDFData, udwfFeat)
    }.toSeq

    val tsFeatData = (udafFeat.feats.nonEmpty, udwfFeatSet.nonEmpty, udfFeat.feats.nonEmpty) match {
      // Handling of cases when one of the types of feature is not requested
      case (true, true, true)   => UDFFuncDispatcher((udafData +: udwfData).joinAllWithoutRepartiton(Seq(entityId), "outer"), udfFeat)
      case (true, true, false)  => (udafData +: udwfData).joinAllWithoutRepartiton(Seq(entityId), "outer")

      case (true, false, true)  => UDFFuncDispatcher(udafData, udfFeat)
      case (true, false, false) => udafData

      case (false, true, true)  => UDFFuncDispatcher(udwfData.joinAllWithoutRepartiton(Seq(entityId), "outer"), udfFeat)
      case (false, true, false) => udwfData.joinAllWithoutRepartiton(Seq(entityId), "outer")

      // case (false,false,true) => getUDFMaterialized(udafData,udfCol) // Note : Not Possible as udfCol like average required aggregates in stage UDAF
      // case (false,false,false) => udafData // Note: will be filtered in the last distinct timeseries stage
    }

    // Only select final output Columns.
    val tsFinalFeatureList = entityId +: parsedFeat.FinalColumns
    tsFeatData.select(tsFinalFeatureList.head, tsFinalFeatureList.tail: _*)
  }

  /**
   * UDAF Functions logic implementer
   *
   * @param data     : Data on which UDAF functions are to be applied
   * @param UDAFFeat : FeatureSet defining the UDAF functions
   * @return : UDAF Column dataframe as specified by UDAFParsed Features
   */
  private def UDAFFuncDispatcher(data: DataFrame, UDAFFeat: UDAFParsed): DataFrame = {
    import UDAFFeat._
    data
      .select(dependency.head, dependency.tail: _*)
      .groupBy(entityId)
      .agg(feats.head, feats.tail: _*)
  }

  /**
   * UDF Functions logic implementer
   *
   * @param data    : Data on which UDF functions are to be applied
   * @param UDFFeat : FeatureSet defining the UDF functions
   * @return : UDF Column dataframe as specified by UDFParsed Features
   */
  private def UDFFuncDispatcher(data: DataFrame, UDFFeat: UDFParsed): DataFrame = {
    val newSetCol = data.columns.map(col) ++ UDFFeat.feats
    data.select(newSetCol: _*)
  }

  /**
   * UDWF Functions logic implementer (Apply window functions first and then apply aggregation functions )
   *
   * @param data     : Data on which UDWF functions are to be applied
   * @param UDWFFeat : FeatureSet defining the UDWF functions
   * @return : UDAF columns dataframe of UDWF features as specified by specsheet
   */
  private def UDWFFuncDispatcher(data: DataFrame, UDWFFeat: UDWFParsed): DataFrame = {

    import UDWFFeat._
    val allCol = dependency.map(col) ++ feats
    data
      .select(dependency.head, dependency.tail: _*) //dependency
      .select(allCol: _*) // UDWF Function
      .groupBy(entityId)
      .agg(udaf.head, udaf.tail: _*)
  }

}
