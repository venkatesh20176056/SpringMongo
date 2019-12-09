package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.DataTables.{gaTableAppRaw, gaTableWebRaw}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.base.Constants._

object AggFeaturesGARaw extends AggFeaturesGARawJob
  with SparkJob with SparkJobBootstrap

trait AggFeaturesGARawJob {
  this: SparkJob =>

  val JobName = "AggFeaturesGARaw"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val dt: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt

    //GA specific dates
    val targetDateOffseted = dt.minusDays(gaDelayOffset) // GA data is available at delay of 2 days
    val dtGASeq = dayIterator(targetDateOffseted.minusDays(gaBackFillDays), targetDateOffseted, ArgsUtils.formatter)

    dtGASeq.foreach(targetDate => {
      import spark.implicits._

      // val out path
      val gaBKViewsFeaturesPath = s"${settings.featuresDfs.baseDFS.aggPath}/GAFeatures/BK/dt=$targetDate/"
      val gaL3RUViewsFeaturesPath = s"${settings.featuresDfs.baseDFS.aggPath}/GAFeatures/L3RU/dt=$targetDate/"
      val gaL2RUViewsFeaturesPath = s"${settings.featuresDfs.baseDFS.aggPath}/GAFeatures/L2RU/dt=$targetDate/"
      println(gaBKViewsFeaturesPath)
      println(gaL3RUViewsFeaturesPath)
      println(gaL2RUViewsFeaturesPath)

      // Read GA Data
      val filterAppCurrentNameBKTravel = Seq("/train", "train-tickets", "/flights", "/flights/search-results", "/flights/review-itenary",
        "/flights/review- itinerary", "/flights/review-itinerary", "/flights/traveler-details", "/flights/traveller-details",
        "/flights/order-summary", "/flights/OrderSummary", "/flights/order-history", "Bus Ticket", "Bus home page", "bus-tickets",
        "busticket_homepage", "Bus Search Screen", "Bus search", "busticket", "Bus Seat Selection Screen", "Bus seat selection",
        "busticket_seatselection", "Bus Passenger Details Screen", "busticket_passenger_details", "Bus Review Itinerary Screen",
        "busticket_confirm_booking")

      val filterRUPageNames = $"app_current_name".like("/gas%") or
        $"app_current_name".like("/recharge%") or
        $"app_current_name".like("/dth%") or
        $"app_current_name".like("/broadband%") or
        $"app_current_name".like("/datacard%") or
        $"app_current_name".like("/electricity%") or
        $"app_current_name".like("/water%") or
        $"app_current_name".like("/gas%") or
        $"app_current_name".like("/landline%") or
        $"app_current_name".like("financial%") or
        $"app_current_name".like("/loan%") or
        $"app_current_name".like("/metro%") or
        $"app_current_name".like("/devotion%") or
        $"app_current_name".like("/google%") or
        $"app_current_name".like("/challan%") or
        $"app_current_name".like("/municipal%") or
        $"app_current_name".like("/toll%") or
        $"app_current_name".isin(filterAppCurrentNameBKTravel: _*)

      var isExecute = true

      val gaData = try {

        //make paths
        val targetDateKey = s"created_at=" + targetDate

        val gaAPPPath = settings.datalakeDfs.gaAppPaths.get(targetDateKey).last.replace("s3:", "s3a:")
        val gaWebPath = settings.datalakeDfs.gaWebPaths.get(targetDateKey).last.replace("s3:", "s3a:")

        gaTableAppRaw(spark, gaAPPPath, filterRUPageNames)
          .withColumn("isApp", lit(1))
          .union(gaTableWebRaw(spark, gaWebPath, filterRUPageNames).withColumn("isApp", lit(0))) //to filter out customers which has no RU evnts
          .repartition($"customer_id")
      } catch {
        case e: Throwable =>
          println(s"Caught Exception while reading GA data and skipping execution for $targetDate. Exception: ${e.getMessage}")
          isExecute = false
          spark.emptyDataFrame
      }

      gaData.cache()
      gaData.count() // forcing the dataframe to cache in memory before group by operations

      if (isExecute) {
        val gaAggregatesL3RU = gaData
          .groupBy("customer_id")
          .agg(
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/gas%"), $"session_identifier").otherwise(null)).as("RU_GAS_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/gas%"), $"session_identifier").otherwise(null)).as("RU_GAS_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/recharge%"), $"session_identifier").otherwise(null)).as("RU_MB_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/recharge%"), $"session_identifier").otherwise(null)).as("RU_MB_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/dth%"), $"session_identifier").otherwise(null)).as("RU_DTH_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/dth%"), $"session_identifier").otherwise(null)).as("RU_DTH_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/broadband%"), $"session_identifier").otherwise(null)).as("RU_BB_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/broadband%"), $"session_identifier").otherwise(null)).as("RU_BB_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/datacard%"), $"session_identifier").otherwise(null)).as("RU_DC_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/datacard%"), $"session_identifier").otherwise(null)).as("RU_DC_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/electricity%"), $"session_identifier").otherwise(null)).as("RU_EC_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/electricity%"), $"session_identifier").otherwise(null)).as("RU_EC_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/water%"), $"session_identifier").otherwise(null)).as("RU_WAT_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/water%"), $"session_identifier").otherwise(null)).as("RU_WAT_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/landline%"), $"session_identifier").otherwise(null)).as("RU_LDL_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/landline%"), $"session_identifier").otherwise(null)).as("RU_LDL_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("financial%"), $"session_identifier").otherwise(null)).as("RU_INS_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("financial%"), $"session_identifier").otherwise(null)).as("RU_INS_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/loan%"), $"session_identifier").otherwise(null)).as("RU_LOAN_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/loan%"), $"session_identifier").otherwise(null)).as("RU_LOAN_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/metro%"), $"session_identifier").otherwise(null)).as("RU_MTC_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/metro%"), $"session_identifier").otherwise(null)).as("RU_MTC_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/devotion%"), $"session_identifier").otherwise(null)).as("RU_DV_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/devotion%"), $"session_identifier").otherwise(null)).as("RU_DV_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/google%"), $"session_identifier").otherwise(null)).as("RU_GP_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/google%"), $"session_identifier").otherwise(null)).as("RU_GP_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/challan%"), $"session_identifier").otherwise(null)).as("RU_CHL_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/challan%"), $"session_identifier").otherwise(null)).as("RU_CHL_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/municipal%"), $"session_identifier").otherwise(null)).as("RU_MNC_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/municipal%"), $"session_identifier").otherwise(null)).as("RU_MNC_total_web_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".like("/toll%"), $"session_identifier").otherwise(null)).as("RU_TOLL_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".like("/toll%"), $"session_identifier").otherwise(null)).as("RU_TOLL_total_web_sessions_count")
          )

        gaAggregatesL3RU
          .coalesce(10)
          .write.mode(SaveMode.Overwrite)
          .parquet(gaL3RUViewsFeaturesPath)

        // BK GA aggregates
        val gaAggregatesBK = gaData
          .groupBy("customer_id")
          .agg(
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("/train", "train-tickets"), $"session_identifier").otherwise(null)).as("BK_TR_total_app_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("/train", "train-tickets"), $"session_identifier").otherwise(null)).as("BK_TR_total_web_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && ($"app_current_name" === "/flights"), $"session_identifier").otherwise(null)).as("BK_FL_total_app_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && ($"app_current_name" === "/flights/search-results"), $"session_identifier").otherwise(null)).as("BK_FL_total_app_searchpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("/flights/review-itenary", "/flights/review- itinerary", "/flights/review-itinerary"), $"session_identifier").otherwise(null)).as("BK_FL_total_app_reviewpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("/flights/traveler-details", "/flights/traveller-details"), $"session_identifier").otherwise(null)).as("BK_FL_total_app_passengerpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("/flights/order-summary", "/flights/OrderSummary", "/flights/order-history"), $"session_identifier").otherwise(null)).as("BK_FL_total_app_orderpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && ($"app_current_name" === "/flights"), $"session_identifier").otherwise(null)).as("BK_FL_total_web_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && ($"app_current_name" === "/flights/search-results"), $"session_identifier").otherwise(null)).as("BK_FL_total_web_searchpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("/flights/review-itenary", "/flights/review- itinerary", "/flights/review-itinerary"), $"session_identifier").otherwise(null)).as("BK_FL_total_web_reviewpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("/flights/traveler-details", "/flights/traveller-details"), $"session_identifier").otherwise(null)).as("BK_FL_total_web_passengerpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("/flights/order-summary", "/flights/OrderSummary", "/flights/order-history"), $"session_identifier").otherwise(null)).as("BK_FL_total_web_orderpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("Bus Ticket", "Bus home page", "bus-tickets", "busticket_homepage"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("Bus Search Screen", "Bus search", "busticket"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_searchpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("Bus Seat Selection Screen", "Bus seat selection", "busticket_seatselection"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_seatpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && $"app_current_name".isin("Bus Passenger Details Screen", "busticket_passenger_details"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_passengerpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && ($"app_current_name" === "Bus Review Itinerary Screen"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_reviewpage_sessions_count"),
            countDistinct(when(($"isApp" === 1) && ($"app_current_name" === "busticket_confirm_booking"), $"session_identifier").otherwise(null)).as("BK_BUS_total_app_orderpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("Bus Ticket", "Bus home page", "bus-tickets", "busticket_homepage"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_homepage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("Bus Search Screen", "Bus search", "busticket"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_searchpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("Bus Seat Selection Screen", "Bus seat selection", "busticket_seatselection"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_seatpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && $"app_current_name".isin("Bus Passenger Details Screen", "busticket_passenger_details"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_passengerpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && ($"app_current_name" === "Bus Review Itinerary Screen"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_reviewpage_sessions_count"),
            countDistinct(when(($"isApp" === 0) && ($"app_current_name" === "busticket_confirm_booking"), $"session_identifier").otherwise(null)).as("BK_BUS_total_web_orderpage_sessions_count")
          )

        gaAggregatesBK
          .coalesce(10)
          .write.mode(SaveMode.Overwrite)
          .parquet(gaBKViewsFeaturesPath)

        // RU L2 aggregates

        val gaAggregatesL2RU = gaData
          .groupBy("customer_id")
          .agg(
            countDistinct(when(($"isApp" === 1) && ($"app_current_name".like("/gas%") or $"app_current_name".like("/recharge%") or $"app_current_name".like("/dth%") or $"app_current_name".like("/broadband%") or $"app_current_name".like("/datacard%") or $"app_current_name".like("/electricity%") or $"app_current_name".like("/water%") or $"app_current_name".like("/gas%") or $"app_current_name".like("/landline%") or $"app_current_name".like("financial%") or $"app_current_name".like("/loan%") or $"app_current_name".like("/metro%") or $"app_current_name".like("/devotion%") or $"app_current_name".like("/google%") or $"app_current_name".like("/challan%") or $"app_current_name".like("/municipal%") or $"app_current_name".like("/toll%")), $"session_identifier").otherwise(null))
              .as("L2GA_RU_total_app_sessions_count"),
            countDistinct(when(($"isApp" === 0) && ($"app_current_name".like("/gas%") or $"app_current_name".like("/recharge%") or $"app_current_name".like("/dth%") or $"app_current_name".like("/broadband%") or $"app_current_name".like("/datacard%") or $"app_current_name".like("/electricity%") or $"app_current_name".like("/water%") or $"app_current_name".like("/gas%") or $"app_current_name".like("/landline%") or $"app_current_name".like("financial%") or $"app_current_name".like("/loan%") or $"app_current_name".like("/metro%") or $"app_current_name".like("/devotion%") or $"app_current_name".like("/google%") or $"app_current_name".like("/challan%") or $"app_current_name".like("/municipal%") or $"app_current_name".like("/toll%")), $"session_identifier").otherwise(null))
              .as("L2GA_RU_total_web_sessions_count")
          )

        gaAggregatesL2RU
          .coalesce(10)
          .write.mode(SaveMode.Overwrite)
          .parquet(gaL2RUViewsFeaturesPath)
      }
    })
  }
}