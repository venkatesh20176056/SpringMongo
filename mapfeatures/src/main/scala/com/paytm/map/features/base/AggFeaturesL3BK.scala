package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.base.Constants._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo.{GAL3BKSchema, L3BKSchema}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.UDFs.{collectUnique, readTableV3, toOperatorCnt}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{first, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object AggFeaturesL3BK extends AggFeaturesL3JobBK
  with SparkJob with SparkJobBootstrap

trait AggFeaturesL3JobBK {
  this: SparkJob =>

  val JobName = "AggFeaturesL3BK"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val startDate = targetDate.minusDays(lookBackDays)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    //GA specific dates
    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at delay of 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val dtGASeq = dayIterator(targetDateGA.minusDays(gaBackFillDays), targetDateGA, ArgsUtils.formatter)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS
    val aggL3BKPath = s"${baseDFS.aggPath}/L3BK"
    val gaAggPath = settings.featuresDfs.baseDFS.gaAggregatePath(targetDateGA)

    val anchorPath = s"${baseDFS.anchorPath}/L3BK"

    println(aggL3BKPath)

    import spark.implicits._
    implicit val sparkSession = spark

    val salesOrderMetaPromo = spark.read.parquet(baseDFS.salesPromMetaPath)
      .where($"dt".between(dtSeq.min, dtSeq.max))
      .withColumnRenamed("channel_id", "transaction_platform")
      .addBKLevel()
      .repartition($"dt", $"customer_id")
      .cache()

    // Generate Sales Aggregate
    val salesAggregates = salesOrderMetaPromo
      .select("customer_id", "dt", "BK_Level", "order_item_id", "selling_price", "created_at", "discount")
      .addSalesAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "BK_Level")

    // Generate Sales First and Last Aggregate
    val firstLastTxn = salesOrderMetaPromo
      .select("customer_id", "dt", "BK_Level", "operator", "selling_price", "created_at")
      .addFirstLastCol(Seq("customer_id", "dt", "BK_Level"), isOperator = true)
      .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "BK_Level", isOperator = true)

    // Preferred Operator Count
    val preferableOperator = salesOrderMetaPromo.addPreferredOperatorFeat("BK_Level")

    //  Count and size Languages For Movies
    val movieLanguagueFeatures = salesOrderMetaPromo.addMovieLangFeat()

    // Movie behavior features
    val movieFeatures = salesOrderMetaPromo.addMovieFeatures(settings, startDateStr, targetDateStr)

    // Flights Features
    val flightsData = salesOrderMetaPromo
      .addFlightFeatures()

    val flightOlap = DataTables.flightsOLAP(spark, settings.datalakeDfs.flights, startDateStr, targetDateStr).cache
    val flightTravelFeatures = travelFeatures(flightOlap, "FL")
      .join(futureTravelFeatures(flightOlap, startDate, targetDate, "FL"), Seq("customer_id", "dt"), "outer")
      .join(nextTravelFeatures(
        DataTables.flightsOLAP(spark, settings.datalakeDfs.flights, startDate.minusMonths(6).toString(ArgsUtils.formatter), targetDateStr),
        startDate, targetDate, "IFL"
      ), Seq("customer_id", "dt"), "outer")

    // Buses Features
    val busData = salesOrderMetaPromo
      .addBusFeatures()

    val busOlap = DataTables.busOLAP(spark, settings.datalakeDfs.buses, startDateStr, targetDateStr).cache
    val busTravelFeatures = travelFeatures(busOlap, "BUS")
      .join(busOlap.addBusPreferableOperator(), Seq("customer_id", "dt"), "outer")
      .join(futureTravelFeatures(busOlap, startDate, targetDate, "BUS"), Seq("customer_id", "dt"), "outer")

    // Trains Features
    val trainData = salesOrderMetaPromo
      .addTrainFeatures()

    val trainOlap = DataTables.trainsOLAP(spark, settings.datalakeDfs.trains, startDateStr, targetDateStr).cache
    val trainTravelFeatures = travelFeatures(trainOlap, "TR")
      .join(futureTravelFeatures(trainOlap, startDate, targetDate, "TR"), Seq("customer_id", "dt"), "outer")

    // Hotels Features
    val hotelsData = salesOrderMetaPromo.addHotelFeatures(settings, startDateStr, targetDateStr)

    // Fees Features
    val feesData = salesOrderMetaPromo.addFeeFeatures()

    // Generate Promocode Aggregates
    val promocodeAggregates = salesOrderMetaPromo
      .where($"isPromoCodeJoin".isNotNull) // Implement Inner Join betqween SO,SOI,Promo,Meta
      .addPromocodeAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "BK_Level")

    // add custom travel behavioral features
    val travelTRBehavioralTable = readTableV3(spark, settings.datalakeDfs.trainsPnrCheck, targetDateStr, startDateStr)
    val travelTRBehavioralAgg = travelTRBehavioralTable
      .groupBy($"customer_id", $"dt")
      .agg(count($"pnr_number").as("TR_pnr_status_check"))

    // Final Aggregates
    configureSparkForMumbaiS3Access(spark)
    val dataDF = {
      salesAggregates
        .join(firstLastTxn, Seq("customer_id", "dt"), "left_outer")
        .join(preferableOperator, Seq("customer_id", "dt"), "left_outer")
        .join(promocodeAggregates, Seq("customer_id", "dt"), "left_outer")
        .join(movieLanguagueFeatures, Seq("customer_id", "dt"), "left_outer")
        .join(movieFeatures, Seq("customer_id", "dt"), "left_outer")
        .join(busData, Seq("customer_id", "dt"), "left_outer")
        .join(flightsData, Seq("customer_id", "dt"), "left_outer")
        .join(trainData, Seq("customer_id", "dt"), "left_outer")
        .join(hotelsData, Seq("customer_id", "dt"), "left_outer")
        .join(flightTravelFeatures, Seq("customer_id", "dt"), "outer")
        .join(busTravelFeatures, Seq("customer_id", "dt"), "outer")
        .join(trainTravelFeatures, Seq("customer_id", "dt"), "outer")
        .join(feesData, Seq("customer_id", "dt"), "left_outer")
        .join(travelTRBehavioralAgg, Seq("customer_id", "dt"), "outer")
        .renameColumns(prefix = "L3_BK_", excludedColumns = Seq("customer_id", "dt"))
        .alignSchema(L3BKSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggL3BKPath)

    // GA Features
    val aggGABasePath = s"${baseDFS.aggPath}/GAFeatures/L3BK/"

    val gaTable = spark.read.parquet(gaAggPath).addBKLevelGA()
    val gaAggregates = gaTable.select(
      "customer_id",
      "dt",
      "BK_Level",
      "product_views_app",
      "product_clicks_app",
      "product_views_web",
      "product_clicks_web",
      "pdp_sessions_app",
      "pdp_sessions_web"
    )
      .addGAAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol = "BK_Level")

    val gaAggregateL3 = gaAggregates
      .renameColumns(prefix = "L3GA_BK_", excludedColumns = Seq("customer_id", "dt"))
      .alignSchema(GAL3BKSchema)
      .coalescePartitions("dt", "customer_id", dtGASeq)
      .cache

    // moving ga features to respective directory
    gaAggregateL3.moveHDFSData(dtGASeq, aggGABasePath)
  }

  def travelFeatures(travelOlap: DataFrame, bkLevel: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    travelOlap
      .where($"travel_date".isNotNull)
      .withColumn("BK_Level", lit(bkLevel))
      .withColumn("travel_day", date_format($"travel_date", "EEE"))
      .withColumn("booking_day", date_format($"dt", "EEE"))
      .select(
        $"BK_Level",
        $"customer_id",
        $"order_id",
        $"travel_day",
        $"booking_day",
        $"dt"
      )
      .distinct
      .addPreferredColumnsFeat("BK_Level", Seq(
        ("travel_day", "preferable_travel_day"),
        ("booking_day", "preferable_booking_day")
      ))
  }

  def nextTravelFeatures(travelOlap: DataFrame, startDate: DateTime, targetDate: DateTime, bkLevel: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val allDates = BaseTableUtils.dayIterator(startDate, targetDate, ArgsUtils.formatter)
      .toDF("dt")
      .withColumn("dt", to_date($"dt"))

    val internationalFlights = travelOlap
      .where($"trip_type" === "International")
      .withColumn("purchase_date", $"dt")
      .drop($"dt")
      .distinct

    val windowByCustomerOrderedByTravelDate = Window.partitionBy($"dt", $"customer_id").orderBy($"travel_date")

    allDates
      .crossJoin(internationalFlights)
      .withColumn("travel_date", when($"dt" < $"purchase_date" or $"dt" >= $"travel_date", null).otherwise($"travel_date"))
      .withColumn("destination_country", when($"travel_date".isNull, null).otherwise($"destination_country"))
      .withColumn(s"${bkLevel}_next_travel_date", first($"travel_date", true).over(windowByCustomerOrderedByTravelDate))
      .withColumn(s"${bkLevel}_next_destination_country", first($"destination_country", true).over(windowByCustomerOrderedByTravelDate))
      .groupBy($"customer_id", $"dt")
      .agg(
        first($"${bkLevel}_next_travel_date", true) as s"${bkLevel}_next_travel_date",
        first($"${bkLevel}_next_destination_country", true) as s"${bkLevel}_next_destination_country"
      )
      .select(
        $"customer_id",
        $"${bkLevel}_next_travel_date",
        $"${bkLevel}_next_destination_country",
        $"dt"
      )
  }

  def futureTravelFeatures(travelOlap: DataFrame, startDate: DateTime, targetDate: DateTime, bkLevel: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val allDates = BaseTableUtils.dayIterator(startDate, targetDate, ArgsUtils.formatter)
      .toDF("dt")
      .withColumn("dt", to_date($"dt"))

    allDates
      .crossJoin(travelOlap.drop($"dt"))
      .withColumn("travel_date", when($"travel_date" > $"dt", $"travel_date").otherwise(null))
      .withColumn("destination_city", when($"travel_date".isNull, null).otherwise($"destination_city"))
      .where($"travel_date".isNotNull and $"destination_city".isNotNull)
      .select(
        $"customer_id",
        $"dt",
        struct($"destination_city", $"travel_date") as s"${bkLevel}_future_travel_detail"
      )
      .groupBy($"customer_id", $"dt")
      .agg(collect_set($"${bkLevel}_future_travel_detail") as s"${bkLevel}_future_travel_details")
  }

  implicit class L3BKImplicits(dF: DataFrame) {

    def addBKLevel(): DataFrame = {
      // ToDo Smita: Vertical id 60 == Hotels. HT=Hospitality. Not sure what vertial id 104 is,
      //  separating out Hotels to pick 'HO' features till this is verified
      import dF.sparkSession.implicits._
      dF.withColumn(
        "BK_Level",
        when($"vertical_id".isin(29, 52, 81), lit("GC"))
          .when($"vertical_id".isin(72), lit("TR"))
          .when($"vertical_id".isin(26), lit("BUS"))
          .when($"vertical_id".isin(64), lit("FL"))
          .when($"vertical_id".isin(70) && ($"name" =!= "Food Items"), lit("MV"))
          .when($"vertical_id".isin(40, 73), lit("EV"))
          .when($"vertical_id".isin(74), lit("AM"))
          .when($"vertical_id".isin(66, 85, 5, 62, 107), lit("DL"))
          .when($"vertical_id".isin(104), lit("HT"))
          .when($"vertical_id".isin(60), lit("HO"))
          .when($"vertical_id".isin(17), lit("FEE"))
          .otherwise(lit(null))
      ).where($"BK_Level".isNotNull)
        .withColumn(
          "operator",
          when($"BK_Level" === "FL", $"flight_operator")
            .when($"BK_Level" === "MV", $"movie_code")
            .when($"BK_Level" === "EV", $"event_entity_id")
            .otherwise($"service_operator")
        )
        .withColumn("BK_Level", when(($"BK_Level" === "FL") && (lower($"flight_type") =!= "domestic"), lit("IFL")).otherwise($"BK_Level"))
        .drop("flight_operator")
        .drop("service_operator")
        .drop("flight_type")
        .drop("event_entity_id")
    }

    def addBKLevelGA(): DataFrame = {
      // ToDo Smita: Vertical id 60 == Hotels. HT=Hospitality. Not sure what vertial id 104 is,
      //  separating out Hotels to pick 'HO' features till this is verified
      import dF.sparkSession.implicits._
      dF.withColumn(
        "BK_Level",
        when($"vertical_id".isin(29, 52, 81), lit("GC"))
          .when($"vertical_id".isin(72), lit("TR"))
          .when($"vertical_id".isin(26), lit("BUS"))
          .when($"vertical_id".isin(64), lit("FL"))
          .when($"vertical_id".isin(70), lit("MV"))
          .when($"vertical_id".isin(40, 73), lit("EV"))
          .when($"vertical_id".isin(74), lit("AM"))
          .when($"vertical_id".isin(66, 85, 6, 62, 107), lit("DL"))
          .when($"vertical_id".isin(104), lit("HT"))
          .when($"vertical_id".isin(60), lit("HO"))
          .otherwise(lit(null))
      ).where($"BK_Level".isNotNull)
    }

    val languageList = Array("English", "Hindi", "Tamil", "Telugu", "Gujarati", "Bengali", "Punjabi", "Marathi", "Kannada", "Malayalam", "Gujarati")

    def addMovieLangFeat(): DataFrame = {

      import dF.sparkSession.implicits._
      dF.where($"BK_Level" === "MV")
        .where($"language".isin(languageList: _*))
        .groupBy("customer_id", "dt")
        .pivot("language")
        .agg(
          count($"order_item_id").as("MV_Transaction_Count"),
          sum($"selling_price").as("MV_Transaction_size")
        )
    }

    def addMovieFeatures(settings: Settings, startDateStr: String, targetDateStr: String): DataFrame = {
      import dF.sparkSession.implicits._
      import utils.UDFs.readTableV3

      val movieProductIds = DataTables.catalogProductTable(dF.sparkSession, settings.datalakeDfs.catalog_product)
        .where($"vertical_id" === 70)
        .select($"product_id")

      val reqSchema = new StructType()
        .add("movie", StringType)
        .add("cinemaId", StringType)
        .add("showTime", StringType)

      // join with sales_order_item again to get fulfillment_req
      val soi: DataFrame = DataTables.soiTable(dF.sparkSession, settings.datalakeDfs.salesOrderItem, targetDateStr, startDateStr)
        .where($"successfulTxnFlag" === 1)
        .withColumn("req", from_json($"fulfillment_req", reqSchema))
        .select(
          $"order_item_id",
          $"req.movie",
          $"req.cinemaId",
          $"req.showTime"
        )

      val movieData = readTableV3(dF.sparkSession, settings.datalakeDfs.moviesData)

      val genre = movieData
        .where('movie_genre =!= "")
        .select('paytmid, explode(split('movie_genre, ",")) as 'movie_genre)
        .distinct
        .select(
          $"paytmid",
          trim($"movie_genre") as "movie_genre"
        )
        .groupBy($"paytmid")
        .agg(collect_set($"movie_genre") as "movie_genre")

      val lang = movieData
        .where('movie_language =!= "" and 'movie_language.isNotNull)
        .select('paytmid as 'movie_code, explode(split('movie_language, ",")) as "movie_language")
        .withColumn("movie_language", initcap(trim('movie_language)))
        .where(initcap(trim('movie_language))
          .isin(languageList: _*))
        .distinct
        .groupBy($"movie_code")
        .agg(collect_set($"movie_language") as "movie_language")

      val fulfillment = readTableV3(dF.sparkSession, settings.datalakeDfs.salesOrderFulfillment, targetDateStr, startDateStr)

      dF
        .where($"BK_Level" === "MV" and $"status" === 7)
        .join(broadcast(movieProductIds), Seq("product_id"), "left_semi")
        .join(soi, "order_item_id")
        .join(genre, $"movie_code" === $"paytmid", "left")
        .join(lang, Seq("movie_code"), "left")
        .join(fulfillment, Seq("order_id"), "left")
        .select(
          $"customer_id",
          $"dt",
          $"dmid",
          trim($"movie") as "movie",
          $"cinemaId",
          date_format($"showTime", "EEE") as "day_of_show",
          when($"dt" < to_date($"showTime"), "yes").otherwise("no") as "books_in_advance",
          $"movie_genre",
          $"movie_language",
          get_json_object($"fulfillment_response", "$.promoData.code") as "promocode",
          get_json_object($"fulfillment_response", "$.promoData.header") as "pass_name",
          to_date(get_json_object($"fulfillment_response", "$.promoData.valid_upto")) as "valid_upto"
        )
        .distinct
        .groupBy($"customer_id", $"dt")
        .agg(
          collect_set(when($"movie".isNull or $"movie" === "", null).otherwise($"movie")) as "MV_movies_watched_list",
          collectUnique(collect_list($"movie_genre")) as "MV_movie_genre_booked_list",
          collectUnique(collect_list($"movie_language")) as "MV_movie_language_booked_list",
          collect_set(when($"cinemaId".isNull, null).otherwise(struct($"cinemaId" as "cinema_id", $"dt" as "transaction_date"))) as "MV_watched_at_cinema_cinemaId_list",
          collect_set(when($"dmid".isNull, null).otherwise(struct($"dmid", $"dt" as "transaction_date"))) as "MV_watched_at_cineplex_dmid_list",
          toOperatorCnt(collect_list($"day_of_show")) as "MV_preferred_day_for_show",
          toOperatorCnt(collect_list($"books_in_advance")) as "MV_books_in_advance",
          collect_set(when($"promocode".isNull, null).otherwise(struct($"promocode", $"pass_name", $"valid_upto"))) as "MV_movie_pass_list"
        )
    }

    def addFlightFeatures() = {
      import dF.sparkSession.implicits._

      val data = dF.where($"BK_Level".isin("FL", "IFL"))
      val preferedFeat = data
        .addPreferredColumnsFeat("BK_Level", Seq(
          ("transaction_platform", "preferable_transaction_platform"),
          ("payment_bank", "preferable_transaction_bank"),
          ("payment_method", "preferable_transaction_payment_method"),
          ("source", "preferable_source"),
          ("destination", "preferable_destination"),
          ("booking_type", "preferable_booking_type")
        ))

      val w = Window.partitionBy("customer_id", "dt")
      data
        .withColumn("advance_purchase", datediff($"dt", to_date($"travel_date")))
        .withColumn("is_infant", ($"infant" > 0).cast(IntegerType))
        .select(
          $"customer_id",
          $"dt",
          $"BK_Level",
          $"is_infant",
          $"advance_purchase",
          first(col("source"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_source"),
          first(col("destination"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_destination")
        )
        .groupBy("customer_id", "dt")
        .pivot("BK_Level")
        .agg(
          sum($"is_infant").as("has_infant"),
          sum($"advance_purchase").as("total_daystojourney"),
          first($"last_source").as("last_source"),
          first($"last_destination").as("last_destination")
        )
        .join(preferedFeat, Seq("customer_id", "dt"), "outer")
    }

    def addTrainFeatures(): DataFrame = {
      import dF.sparkSession.implicits._
      val data = dF.where($"BK_Level" === "TR")
      val preferedFeat = data
        .addPreferredColumnsFeat("BK_Level", Seq(
          ("transaction_platform", "preferable_transaction_platform"),
          ("payment_bank", "preferable_transaction_bank"),
          ("payment_method", "preferable_transaction_payment_method"),
          ("source", "preferable_boardingstation"),
          ("destination", "preferable_destinationstation"),
          ("class", "preferable_class")
        ))

      val w = Window.partitionBy("customer_id", "dt")

      data
        .withColumn("advance_purchase", datediff($"dt", to_date(from_unixtime(unix_timestamp($"travel_date", "yyyyMMdd")))))
        .select(
          $"customer_id",
          $"dt",
          $"BK_Level",
          $"distance",
          $"advance_purchase",
          first(col("source"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_source"),
          first(col("destination"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_destination")
        )
        .groupBy("customer_id", "dt")
        .pivot("BK_Level")
        .agg(
          sum($"distance").as("total_distance_per_trip"),
          sum($"advance_purchase").as("total_daystojourney"),
          first($"last_source").as("last_source"),
          first($"last_destination").as("last_destination")
        )
        .join(preferedFeat, Seq("customer_id", "dt"), "outer")
    }

    def addBusFeatures(): DataFrame = {
      import dF.sparkSession.implicits._
      val data = dF.where($"BK_Level" === "BUS")
      val preferedFeat = data
        .addPreferredColumnsFeat("BK_Level", Seq(
          ("transaction_platform", "preferable_transaction_platform"),
          ("payment_bank", "preferable_transaction_bank"),
          ("payment_method", "preferable_transaction_payment_method"),
          ("source", "preferable_source"),
          ("destination", "preferable_destination"),
          ("boarding_point_id", "preferable_boardingpoint")
        ))

      val w = Window.partitionBy("customer_id", "dt")

      data
        .withColumn("advance_purchase", datediff($"dt", to_date($"travel_date")))
        .select(
          $"customer_id",
          $"dt",
          $"BK_Level",
          $"advance_purchase",
          first(col("source"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_source"),
          first(col("destination"), ignoreNulls = true).over(w.orderBy(col("created_at").desc)).as("last_destination")
        ).groupBy("customer_id", "dt")
        .pivot("BK_Level")
        .agg(
          sum($"advance_purchase").as("total_daystojourney"),
          first($"last_source").as("last_source"),
          first($"last_destination").as("last_destination")
        )
        .join(preferedFeat, Seq("customer_id", "dt"), "outer")
    }

    def addBusPreferableOperator(): DataFrame = {
      import dF.sparkSession.implicits._
      dF
        .withColumn("BK_Level", lit("BUS"))
        .select(
          $"BK_Level",
          $"customer_id",
          $"order_id",
          $"operator",
          $"dt"
        )
        .distinct
        .addPreferredColumnsFeat("BK_Level", Seq(
          ("operator", "preferable_transaction_operator")
        ))
    }

    def addHotelFeatures(settings: Settings, startDateStr: String, targetDateStr: String): DataFrame = {
      import dF.sparkSession.implicits._

      val reqSchema = new StructType().add("city", StringType)

      // Get city from sales order item
      val soi: DataFrame = DataTables.soiTable(dF.sparkSession, settings.datalakeDfs.salesOrderItem, targetDateStr, startDateStr)
        .where($"successfulTxnFlag" === 1)
        .withColumn("req", from_json($"fulfillment_req", reqSchema))
        .select(
          $"order_item_id",
          $"req.city" as "city"
        )
        .distinct

      val data = dF.where($"BK_Level" === "HO")

      data
        .join(soi, "order_item_id")
        .select(
          $"BK_Level",
          $"customer_id",
          $"dt",
          $"city"
        )
        .addPreferredColumnsFeat("BK_Level", Seq(
          ("city", "preferable_city")
        ))
    }

    def addFeeFeatures(): DataFrame = {
      import dF.sparkSession.implicits._
      val data = dF.where($"BK_Level" === "FEE")

      data
        .groupBy("customer_id", "dt")
        .agg(
          collect_set("product_id").as("FEE_pid_list"),
          collect_set("dmid").as("FEE_dmid_list")
        )
    }
  }

}
