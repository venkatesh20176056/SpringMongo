package com.paytm.map.features.datasets

import java.sql.Timestamp

import com.paytm.map.features.base.BaseTableUtils.{OperatorDueDate, PromoUsageBase, SMS}
import com.paytm.map.features.utils.UDFs.isAllNumeric
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions._

object Constants {
  val l1 = "PAYTM"
  val l2RU = "RU"
  val l2BK = "BK"
  val l2EC = "EC"
  val l3RU: Seq[String] = Seq("MB_prepaid", "MB_postpaid", "DTH", "BB", "DC", "EC", "WAT", "GAS", "CBL", "LDL", "INS", "LOAN", "MTC", "MT", "DV", "FT", "GP", "CHL", "MNC", "TOLL").map("RU_" + _)
  // ToDo Smita: HT is Hospitality, which already includes Hotels along with another vertical. Separating out Hotels till this is confirmed
  val l3BK: Seq[String] = Seq("TR", "BUS", "FL", "IFL", "MV", "EV", "AM", "DL", "GC", "HT", "FEE", "HO").map("BK_" + _)
  val l3EC1: Seq[String] = Seq("GNS", "WF", "OGNS", "OHNK", "HNK", "HNW", "BS", "BNPC", "GIFTS", "PC", "ELC", "MNA", "STN", "EH", "ZIP", "AUTO", "IS", "GF", "GC", "IB", "MMNTV", "MF", "SS", "BKNT", "OC", "SNH", "MT", "BOOKS", "ANP", "SM", "CNB", "OS").map("EC1_" + _)
  val l3EC2_I: Seq[String] = Seq("51396", "74601", "49120", "12739", "6452", "8981", "10022", "5276", "5254", "51552", "51599", "24717", "23192", "6224", "6304", "101401", "6301", "101405", "5029", "114316", "26120").map("EC2_CAT" + _) //as requested by business https://jira.mypaytm.com/browse/MAP-450
  val l3EC2_II: Seq[String] = Seq("6498", "7908", "6443", "5048", "49090", "23095", "8027", "5041", "5267", "5180", "5187", "6299", "78462", "5246", "8996", "5240", "26126", "78457", "26122", "101311", "7467", "40038", "40044", "6240").map("EC2_CAT" + _) //as requested by business https://jira.mypaytm.com/browse/MAP-450
  val l3EC3: Seq[String] = Seq("5928", "5038", "78658", "101416", "101332", "101533", "14886", "78654", "101532", "101383").map("EC3_CAT" + _) // as requested by business https://jira.mypaytm.com/browse/MAP-450
  val l3EC4_I: Seq[String] = Seq("125120", "60009", "6309", "6453", "11420", "6241", "23633", "6244", "6373", "6492", "28996", "6242", "5030", "8543", "8544", "5035", "5241", "5242", "5053", "5049", "5050", "5413", "77978", "5057", "8997", "66781", "6499", "24838", "24843", "29161", "24837", "24194", "6502", "27601", "17628", "24836", "26400", "15627", "5238", "5932", "5846", "6017", "8139", "101444").map("EC4_CAT" + _) // as requested by business https://jira.mypaytm.com/browse/MAP-450
  val l3EC4_II: Seq[String] = Seq("7710", "6501", "30193", "30161", "101450", "101440", "101455", "101384", "101387", "101390", "101385", "101391", "101449", "101431", "101425", "101504", "101381", "101395", "101325", "101322", "101326", "101551", "101544", "101545", "101467", "101471", "101472", "101396", "101375", "101315", "101460", "101452", "101554", "101420", "101333", "101553", "15343", "101556", "101323", "40343", "26174", "14887", "8732", "6451").map("EC4_CAT" + _) // as requested by business https://jira.mypaytm.com/browse/MAP-450
  val l3EC4_III: Seq[String] = Seq("101314", "101509", "101419", "27220", "11641", "25126", "6372", "6375", "6374", "18231", "83048", "32596", "8591", "8028", "8029", "8030", "9182", "15594", "15595", "24839", "27221", "30041", "30164", "30165", "30725", "48721", "81148", "27620", "67574", "5232", "7445", "8731", "6036", "66780", "66782", "66783", "5684", "41830", "19686", "5037", "5233", "89695", "101378").map("EC4_CAT" + _) // as requested by business https://jira.mypaytm.com/browse/MAP-450
  val gaBK = "GABK"
  val gaL3RU = "GAL3RU"
  val gaL3EC1 = "GAL3EC1"
  val gaL3EC2 = "GAL3EC2"
  val gaL3EC3 = "GAL3EC3"
  val gaL3EC4 = "GAL3EC4"
  val gaL3BK = "GAL3BK"
  val gaL2RU = "GAL2RU"
  val gaL2EC = "GAL2EC"
  val gaL2BK = "GAL2BK"
  val gaL1 = "GAPAYTM"
  val gaL3BKTravel: Seq[String] = Seq("TR", "BUS", "FL").map("BK_" + _)
  val gaL3BKTravelMap: Map[String, Seq[String]] = Map(
    "BK_TR" -> Seq("homepage"),
    "BK_FL" -> Seq("homepage", "orderpage", "passengerpage", "reviewpage", "searchpage"),
    "BK_BUS" -> Seq("homepage", "orderpage", "passengerpage", "reviewpage", "searchpage", "seatpage")
  )
  val payments = "payments"
  val online = "online"
  val wallet = "wallet"
  val bank = "bank"
  val bills = "Bills"
  val postPaid = "PostPaid"
  val medical = "MEDICAL"
  val goldWallet = "GoldWallet"
  val upi = "UPI"
  val sms = "SMS"
  val gold = "gold"
  val superCashback = "SCB"
  val gaGold = "GAGOLD"
  val lmsPostpaid = "LMS_POSTPAID"
  val lmsPostpaidV2 = "LMS_POSTPAIDV2"
  val lastSeen = "LS"
  val Push = "PUSH"
  val gaFS = "GAFS"
  val gamepind = "GAMEPIND"
  val gatravel = "GATravel"
  val insurance = "INSURANCE"
  val postpaid = "POSTPAID"
  val device = "DEVICE"
  val location = "LOCATION"
  val distanceAndSpeed = "DistanceAndSpeed"
  val customerDevice = "CustomerDeviceFeatures"
  val signal = "SIGNAL"
  val paytmFirstDropped = "PaytmFirstDropped"
  val overall = "OVERALL"
  val l1l2levels = Seq(l1, l2RU, l2BK, l2EC)
  val noDiscountVerticals: Seq[String] = Seq("RU_CBL", "RU_FT") ++ l3EC3 ++ l3EC4_I ++ l3EC4_II ++ l3EC4_III
  val noCashbackVerticals: Seq[String] = l3EC3 ++ l3EC4_I ++ l3EC4_II ++ l3EC4_III
  val languageVerticals: Seq[String] = Seq("BK_English_MV", "BK_Hindi_MV", "BK_Tamil_MV", "BK_Telugu_MV")
  val travelVerticals: Seq[String] = Seq("TR", "BUS", "FL", "IFL").map("BK_" + _)
  val noPreferredDayLevelPrefixes: Seq[String] = Seq("EC3", "EC4")
  val timeSeriesDurations = Seq(180, 90, 30, 7, 3, 1)
  val timeSeriesDurationsEC3EC4 = Seq(90, 30, 15, 7)
  val monthlyAggDurations = Seq(0, 1)
  val gaTimeSeriesDurations = Seq(60, 30)
  val gaTimeSeriesDurationsRevised = Seq(15, 7, 3, 1)
  val gaTimeSeriesPrefixes = Seq("GAL2EC", "GAL3EC1", "GAL3EC2", "GAL3EC3", "GAL3EC4") // For these levelPrefixes calculate TS features
  val gaDerivedFeatPrefixes = Seq("GAL2EC", "GAL3EC1", "GAL3EC2", "GAL3EC3", "GAL3EC4")
  val numOfDecimalPlaces = 3

  val segmentationVal = Array(0, 3, 6, 24)
  val groupList = Array("1", "2", "3", "4")
  val langSets = Set("ta-in", "kn-in", "en-in",
    "or-in", "hi-in", "gu-in",
    "ml-in", "bn-in", "te-in",
    "pa-in", "mr-in")

  val gaDatesLookback = 90
  val gaDatesLookbackRevised = 30

  def getKey: Seq[String] = {
    // Key to groupby all datasets on
    Seq("customer_id")
  }

  def getPrefDayKey: Seq[String] = {
    // for preferred day aggregations
    Seq("customer_id", "day_of_week")
  }

  def getAllAgg(levelPrefix: String): Seq[(String, String)] = {
    // Get discount aggregations
    val discountAgg = if (noDiscountVerticals.contains(levelPrefix))
      Seq()
    else {
      Seq(
        ("sum", s"${levelPrefix}_total_discount_transaction_count as ${levelPrefix}_total_discount_transaction_count"),
        ("sum", s"${levelPrefix}_total_discount_transaction_size  as ${levelPrefix}_total_discount_transaction_size")
      )
    }

    // Get cashback aggregations
    val cashbackAgg = if (noCashbackVerticals.contains(levelPrefix)) {
      Seq()
    } else {
      Seq(
        ("sum", s"${levelPrefix}_total_cashback_transaction_count as ${levelPrefix}_total_cashback_transaction_count"),
        ("sum", s"${levelPrefix}_total_cashback_transaction_size  as ${levelPrefix}_total_cashback_transaction_size")
      )
    }

    // Get operator aggregations for L3BK or L3RU
    val allOperatorAgg = if (l3BK.contains(levelPrefix) || l3RU.contains(levelPrefix)) {
      Seq(("collect_as_list", s"$levelPrefix as ${levelPrefix}_operators"))
    } else Seq()

    // Get operator aggregations for EC brand level
    val allOperatorAggEC = if (l3EC1.contains(levelPrefix) || l3EC2_I.contains(levelPrefix) || l3EC2_II.contains(levelPrefix)) {
      Seq(
        ("collect_as_list", s"${levelPrefix}_preferred_brand_count as ${levelPrefix}_operators_bycount"),
        ("collect_as_list", s"${levelPrefix}_preferred_brand_size as ${levelPrefix}_operators_bysize")
      )
    } else Seq()

    // Get aggregations for Travel verticals in BK
    // preferable_{boardingstation, destinationstation, class, transaction_bank, transaction_payment_method, transaction_platform

    val commonTravelOperatorAggregates = Seq(
      ("collect_as_list", s"${levelPrefix}_preferable_transaction_bank as ${levelPrefix}_operators_transaction_bank"),
      ("collect_as_list", s"${levelPrefix}_preferable_transaction_payment_method as ${levelPrefix}_operators_transaction_payment_method"),
      ("collect_as_list", s"${levelPrefix}_preferable_transaction_platform as ${levelPrefix}_operators_transaction_platform")
    )

    val allOperatorAggTravelBK = levelPrefix match {

      case "BK_TR" => Seq(
        ("collect_as_list", s"${levelPrefix}_preferable_boardingstation as ${levelPrefix}_operators_boardingstation"),
        ("collect_as_list", s"${levelPrefix}_preferable_destinationstation as ${levelPrefix}_operators_destinationstation"),
        ("collect_as_list", s"${levelPrefix}_preferable_class as ${levelPrefix}_operators_class")
      ) ++ commonTravelOperatorAggregates

      case "BK_FL" | "BK_IFL" => Seq(
        ("collect_as_list", s"${levelPrefix}_preferable_source as ${levelPrefix}_operators_source"),
        ("collect_as_list", s"${levelPrefix}_preferable_destination as ${levelPrefix}_operators_destination"),
        ("collect_as_list", s"${levelPrefix}_preferable_booking_type as ${levelPrefix}_operators_booking_type")
      ) ++ commonTravelOperatorAggregates

      case "BK_BUS" => Seq(
        ("collect_as_list", s"${levelPrefix}_preferable_source as ${levelPrefix}_operators_source"),
        ("collect_as_list", s"${levelPrefix}_preferable_destination as ${levelPrefix}_operators_destination"),
        ("collect_as_list", s"${levelPrefix}_preferable_boardingpoint as ${levelPrefix}_operators_boardingpoint")
      ) ++ commonTravelOperatorAggregates

      case _ => Seq()

    }

    // Get movie language aggregations
    val movieLangs = languageVerticals
    val moviesAgg = if (levelPrefix.contains("_MV")) {
      val allLangAgg = for (lang <- movieLangs) yield {
        //TODO V2: Correct this in base table to have uniform feature names
        Seq(
          ("sum", s"${lang}_Transaction_Count as ${lang}_total_transaction_count"),
          ("sum", s"${lang}_Transaction_size  as ${lang}_total_transaction_size")
        )
      }
      allLangAgg.flatten
    } else Seq()

    // Get transactional aggregations
    val txnAgg = Seq(
      ("sum", s"${levelPrefix}_total_transaction_count as ${levelPrefix}_total_transaction_count"),
      ("sum", s"${levelPrefix}_total_transaction_size  as ${levelPrefix}_total_transaction_size"),
      ("min", s"${levelPrefix}_first_transaction_date  as ${levelPrefix}_first_transaction_date"),
      ("max", s"${levelPrefix}_last_transaction_date   as ${levelPrefix}_last_transaction_date")
    )
    // aggregations for last date order feature
    val lastTxnDateAgg = if (levelPrefix.contains("RU")) {
      Seq(
        ("max", s"${levelPrefix}_last_attempt_order_date   as ${levelPrefix}_last_attempt_order_date")
      )
    } else Seq()

    val travelAgg = if (travelVerticals.contains(levelPrefix)) {
      Seq(
        ("sum", s"${levelPrefix}_total_daystojourney as ${levelPrefix}_total_daystojourney")
      )
    } else Seq()

    // Get extra aggregations
    val extraAgg = levelPrefix match {
      case `l1`    => Seq(("collect_as_list", s"${levelPrefix}_promocodes_used as ${levelPrefix}_promocodes_used"))
      case "BK_MV" => Seq(("collect_as_list", s"$levelPrefix as ${levelPrefix}_IDs"))
      case "BK_EV" => Seq(("collect_as_list", s"$levelPrefix as ${levelPrefix}_IDs"))
      case "BK_FEE" => Seq(
        ("collect_as_list", s"${levelPrefix}_pid_list as ${levelPrefix}_pid_list"),
        ("collect_as_list", s"${levelPrefix}_dmid_list as ${levelPrefix}_dmid_list")
      )
      case "BK_TR"  => Seq(("sum", s"${levelPrefix}_total_distance_per_trip as ${levelPrefix}_total_distance_per_trip"))
      case "BK_FL"  => Seq(("sum", s"${levelPrefix}_has_infant as ${levelPrefix}_has_infant"))
      case "BK_IFL" => Seq(("sum", s"${levelPrefix}_has_infant as ${levelPrefix}_has_infant"))
      case _        => Seq()
    }

    // Combine all aggregations
    discountAgg ++ cashbackAgg ++ moviesAgg ++ allOperatorAgg ++ allOperatorAggEC ++ allOperatorAggTravelBK ++ travelAgg ++ txnAgg ++ extraAgg ++ lastTxnDateAgg
  }

  def getGAAgg(levelPrefix: String): Seq[(String, String)] = {

    val gaProductAgg =
      Seq(
        ("sum", s"${levelPrefix}_total_product_views_app as ${levelPrefix}_total_product_views_app"),
        ("sum", s"${levelPrefix}_total_product_views_web as ${levelPrefix}_total_product_views_web"),
        ("sum", s"${levelPrefix}_total_product_clicks_app as ${levelPrefix}_total_product_clicks_app"),
        ("sum", s"${levelPrefix}_total_product_clicks_web as ${levelPrefix}_total_product_clicks_web"),
        ("sum", s"${levelPrefix}_total_pdp_sessions_app as ${levelPrefix}_total_pdp_sessions_app"),
        ("sum", s"${levelPrefix}_total_pdp_sessions_web as ${levelPrefix}_total_pdp_sessions_web")
      )

    // landing pages aggregates
    val gaLPAgg =
      Seq(
        ("sum", s"${levelPrefix}_total_clp_sessions_app as ${levelPrefix}_total_clp_sessions_app"),
        ("sum", s"${levelPrefix}_total_clp_sessions_web as ${levelPrefix}_total_clp_sessions_web"),
        ("sum", s"${levelPrefix}_total_glp_sessions_app as ${levelPrefix}_total_glp_sessions_app"),
        ("sum", s"${levelPrefix}_total_glp_sessions_web as ${levelPrefix}_total_glp_sessions_web")
      )

    // for EC1 and EC2 including landing page aggregates along with product aggregates
    val gaAgg =
      if (levelPrefix.startsWith("EC1_") | levelPrefix.startsWith("EC2_") | levelPrefix.startsWith("EC3_") | levelPrefix.startsWith("EC4_")) gaLPAgg ++ gaProductAgg
      else gaProductAgg

    gaAgg
  }

  def getTimeSeriesGAAgg(levelPrefix: String, nDays: Int): Seq[(String, String)] = {

    val gaProductTSAgg =
      Seq(
        ("sum", s"${levelPrefix}_total_product_views_app as ${levelPrefix}_total_product_views_app_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_product_views_web as ${levelPrefix}_total_product_views_web_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_product_clicks_app as ${levelPrefix}_total_product_clicks_app_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_product_clicks_web as ${levelPrefix}_total_product_clicks_web_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_pdp_sessions_app as ${levelPrefix}_total_pdp_sessions_app_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_pdp_sessions_web as ${levelPrefix}_total_pdp_sessions_web_${nDays}_days")
      )

    // landing pages aggregates
    val gaLPTSAgg =
      Seq(
        ("sum", s"${levelPrefix}_total_clp_sessions_app as ${levelPrefix}_total_clp_sessions_app_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_clp_sessions_web as ${levelPrefix}_total_clp_sessions_web_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_glp_sessions_app as ${levelPrefix}_total_glp_sessions_app_${nDays}_days"),
        ("sum", s"${levelPrefix}_total_glp_sessions_web as ${levelPrefix}_total_glp_sessions_web_${nDays}_days")
      )

    val gaTSAgg =
      if (levelPrefix.startsWith("EC1_") | levelPrefix.startsWith("EC2_") | levelPrefix.startsWith("EC3_") | levelPrefix.startsWith("EC4_")) gaLPTSAgg ++ gaProductTSAgg
      else gaProductTSAgg

    gaTSAgg
  }

  def getGARUAgg(levelPrefix: String): Seq[(String, String)] = {
    Seq(
      ("sum", s"${levelPrefix}_total_web_sessions_count as ${levelPrefix}_total_web_sessions_count"),
      ("sum", s"${levelPrefix}_total_app_sessions_count as ${levelPrefix}_total_app_sessions_count")
    )
  }

  def getTimeSeriesGARUAgg(levelPrefix: String, nDays: Int): Seq[(String, String)] = {
    Seq(
      ("sum", s"${levelPrefix}_total_web_sessions_count as ${levelPrefix}_total_web_sessions_count_${nDays}_days"),
      ("sum", s"${levelPrefix}_total_app_sessions_count as ${levelPrefix}_total_app_sessions_count_${nDays}_days")
    )
  }

  def getGATravelAgg(levelPrefix: String): Seq[(String, String)] = {
    // Get ga aggregations
    val gaAgg = if (gaL3BKTravelMap.keys.toSeq.contains(levelPrefix)) {
      // get corresponding pages per levelPrefix on which ga features are required
      val gaURLs = gaL3BKTravelMap(levelPrefix)
      // Geting web and app specific aggregations
      gaURLs.map(pageUrl => {
        Seq(
          ("sum", s"${levelPrefix}_total_web_${pageUrl}_sessions_count as ${levelPrefix}_total_web_${pageUrl}_sessions_count"),
          ("sum", s"${levelPrefix}_total_app_${pageUrl}_sessions_count as ${levelPrefix}_total_app_${pageUrl}_sessions_count")
        )
      }).flatten
    } else {
      Seq()
    }
    gaAgg
  }

  def getTimeSeriesGATravelAgg(levelPrefix: String, nDays: Int): Seq[(String, String)] = {
    // Get ga aggregations
    val gaAgg = if (gaL3BKTravelMap.keys.toSeq.contains(levelPrefix)) {
      // get corresponding pages per levelPrefix on which ga features are required
      val gaURLs = gaL3BKTravelMap(levelPrefix)
      // Geting web and app specific aggregations
      gaURLs.map(pageUrl => {
        Seq(
          ("sum", s"${levelPrefix}_total_web_${pageUrl}_sessions_count as ${levelPrefix}_total_web_${pageUrl}_sessions_count_${nDays}_days"),
          ("sum", s"${levelPrefix}_total_app_${pageUrl}_sessions_count as ${levelPrefix}_total_app_${pageUrl}_sessions_count_${nDays}_days")
        )
      }).flatten
    } else {
      Seq()
    }
    gaAgg
  }

  def getLMSAgg: Seq[(String, String)] = {
    Seq(
      ("collect_as_list", s"account_details as account_details")
    )
  }

  def getAllAggAll(verticals: Seq[String]): Seq[(String, String)] = {
    verticals.flatMap(getAllAgg)
  }

  def getTimeSeriesSalesAgg(levelPrefix: String, nDays: Int): Seq[(String, String)] = {
    // Get aggregation for time series features
    val tsAgg = Seq(
      ("sum", s"${levelPrefix}_total_transaction_count as ${levelPrefix}_total_transaction_count_${nDays}_days"),
      ("sum", s"${levelPrefix}_total_transaction_size as ${levelPrefix}_total_transaction_size_${nDays}_days")
    )

    val feeExtraAgg = Seq(
      ("collect_as_list", s"${levelPrefix}_pid_list as ${levelPrefix}_pid_list_${nDays}_days"),
      ("collect_as_list", s"${levelPrefix}_dmid_list as ${levelPrefix}_dmid_list_${nDays}_days")
    )

    if ((levelPrefix == "BK_FEE") && (nDays == 180)) tsAgg ++ feeExtraAgg else tsAgg

  }

  def getTimeSeriesMonthlySalesAgg(levelPrefix: String, nMonths: Int): Seq[(String, String)] = {
    val semanticMonth = nMonths match {
      case 0 => "this"
      case 1 => "last"
    }

    Seq(
      ("sum", s"${levelPrefix}_total_transaction_count as ${levelPrefix}_total_transaction_count_${semanticMonth}_month"),
      ("sum", s"${levelPrefix}_total_transaction_size as ${levelPrefix}_total_transaction_size_${semanticMonth}_month")
    )
  }

  def getTimeSeriesSalesAggAll(verticals: Seq[String], nDays: Int): Seq[(String, String)] = {
    // Get aggregation for time series features
    verticals.flatMap(x => getTimeSeriesSalesAgg(x, nDays))
  }

  def getMonthlyTimeSeriesSalesAggAll(verticals: Seq[String], nMonths: Int): Seq[(String, String)] = {
    // Get aggregation for time series features
    verticals.flatMap(x => getTimeSeriesMonthlySalesAgg(x, nMonths))
  }

  def getTimeSeriesGAAggAll(levelPrefix: String, verticals: Seq[String], nDays: Int): Seq[(String, String)] = {

    // Get aggregation for time series features
    val aggAllGa =
      if (levelPrefix == "GABK") {
        verticals.flatMap(x => getTimeSeriesGATravelAgg(x, nDays))

      } else if (levelPrefix == "GAL2RU" | levelPrefix == "GAL3RU") {
        verticals.flatMap(x => getTimeSeriesGARUAgg(x, nDays))

      } else {
        verticals.flatMap(x => getTimeSeriesGAAgg(x, nDays))
      }

    aggAllGa
  }

  def getPrefDayAgg(levelPrefix: String): Seq[(String, String)] = {
    Seq(("sum", s"${levelPrefix}_total_transaction_count as count_by_day"))
  }

  def getSortByCol(isFirst: Boolean, levelPrefix: String): String = {
    if (isFirst) s"${levelPrefix}_first_transaction_date" else s"${levelPrefix}_last_transaction_date"
  }

  def getSelectCol(levelPrefix: String, isFirst: Boolean): Seq[String] = {
    if (isFirst)
      Seq(s"${levelPrefix}_first_transaction_size as ${levelPrefix}_first_transaction_size")
    else
      Seq(s"${levelPrefix}_last_transaction_size as ${levelPrefix}_last_transaction_size")
  }

  def getFirstCol(levelPrefix: String): Seq[String] = {
    Seq(
      s"${levelPrefix}_first_transaction_size as ${levelPrefix}_first_transaction_size",
      s"${levelPrefix}_first_transaction_operator as ${levelPrefix}_first_transaction_operator"
    )
  }

  def getLastCol(levelPrefix: String): Seq[String] = {
    val commonLastColumns = Seq(
      s"${levelPrefix}_last_transaction_size as ${levelPrefix}_last_transaction_size",
      s"${levelPrefix}_last_transaction_operator as ${levelPrefix}_last_transaction_operator"
    )

    if (travelVerticals.contains(levelPrefix)) {
      Seq(
        s"${levelPrefix}_last_source as ${levelPrefix}_last_source",
        s"${levelPrefix}_last_destination as ${levelPrefix}_last_destination"
      ) ++ commonLastColumns
    } else commonLastColumns
  }

  def getPrefDayCol(levelPrefix: String): Seq[String] = {
    Seq(s"day_of_week as ${levelPrefix}_preferable_transaction_day")
  }

  def getPrefDayColAll(verticals: Seq[String]): Seq[String] = {
    verticals.flatMap(getPrefDayCol)
  }

  def getCashbackPercents(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    val cashVerticals = cols.diff(noCashbackVerticals)
    for (verticals <- cashVerticals) yield {
      Seq(
        (s"${verticals}_percentage_cashback_by_transaction_count", s"${verticals}_total_cashback_transaction_count", s"${verticals}_total_transaction_count"),
        (s"${verticals}_percentage_cashback_by_transaction_size", s"${verticals}_total_cashback_transaction_size", s"${verticals}_total_transaction_size")
      )
    }
  }

  def getDiscountPercents(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    val discountVerticals = cols.diff(noDiscountVerticals)
    for (verticals <- discountVerticals) yield {
      Seq(
        (s"${verticals}_percentage_discount_by_transaction_count", s"${verticals}_total_discount_transaction_count", s"${verticals}_total_transaction_count"),
        (s"${verticals}_percentage_discount_by_transaction_size", s"${verticals}_total_discount_transaction_size", s"${verticals}_total_transaction_size")
      )
    }
  }

  def getTravelAverageLogic(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    val availableTravelVerticals = cols.intersect(travelVerticals)

    for (vertical <- availableTravelVerticals) yield {
      vertical match {
        case "BK_TR" => Seq(
          (s"${vertical}_avg_daystojourney", s"${vertical}_total_daystojourney", s"${vertical}_total_transaction_count"),
          (s"${vertical}_avg_distance_per_trip", s"${vertical}_total_distance_per_trip", s"${vertical}_total_transaction_count")
        )
        case "BK_IFL" | "BK_FL" => Seq(
          (s"${vertical}_avg_daystojourney", s"${vertical}_total_daystojourney", s"${vertical}_total_transaction_count"),
          (s"${vertical}_avg_infant_per_journey", s"${vertical}_has_infant", s"${vertical}_total_transaction_count")
        )
        case "BK_BUS" => Seq(
          (s"${vertical}_avg_daystojourney", s"${vertical}_total_daystojourney", s"${vertical}_total_transaction_count")
        )
      }
    }
  }

  def getAverageLogic(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    for (levelPrefix <- cols) yield {

      val EC3EC4Verticals = l3EC3 ++ l3EC4_I ++ l3EC4_II ++ l3EC4_III
      val EC1EC2Verticals = l3EC1 ++ l3EC2_I ++ l3EC2_II

      val tsDurations = if (EC3EC4Verticals.contains(levelPrefix)) timeSeriesDurationsEC3EC4
      else if (EC1EC2Verticals.contains(levelPrefix)) timeSeriesDurations ++ Seq(15)
      else timeSeriesDurations

      Seq(
        (s"${levelPrefix}_avg_transaction_size", s"${levelPrefix}_total_transaction_size", s"${levelPrefix}_total_transaction_count")
      ) ++
        (for (days <- tsDurations) yield (s"${levelPrefix}_avg_transaction_size_${days}_days", s"${levelPrefix}_total_transaction_size_${days}_days", s"${levelPrefix}_total_transaction_count_${days}_days"))
    }
  }

  def getGAAggregationLogic(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    for (levelPrefix <- cols) yield {

      val EC3EC4Verticals = l3EC3 ++ l3EC4_I ++ l3EC4_II ++ l3EC4_III
      val EC1EC2Verticals = l3EC1 ++ l3EC2_I ++ l3EC2_II

      val tsDurations = if (EC3EC4Verticals.contains(levelPrefix)) gaTimeSeriesDurationsRevised
      else if (EC1EC2Verticals.contains(levelPrefix)) gaTimeSeriesDurationsRevised ++ gaTimeSeriesDurations
      else gaTimeSeriesDurations

      // aggregating app and web features for ga

      val aggGAtsProperties: Seq[(String, String, String)] = tsDurations.map(days =>
        {
          Seq(
            (s"${levelPrefix}_total_glp_sessions_agg_${days}_days", s"${levelPrefix}_total_glp_sessions_app_${days}_days", s"${levelPrefix}_total_glp_sessions_web_${days}_days"),
            (s"${levelPrefix}_total_pdp_sessions_agg_${days}_days", s"${levelPrefix}_total_pdp_sessions_app_${days}_days", s"${levelPrefix}_total_pdp_sessions_web_${days}_days")
          )
        }).flatten

      Seq(
        (s"${levelPrefix}_total_glp_sessions_agg", s"${levelPrefix}_total_glp_sessions_app", s"${levelPrefix}_total_glp_sessions_web"),
        (s"${levelPrefix}_total_pdp_sessions_agg", s"${levelPrefix}_total_pdp_sessions_app", s"${levelPrefix}_total_pdp_sessions_web")
      ) ++
        aggGAtsProperties

    }
  }

  def getDivisionsLogic(cols: Seq[String]): Seq[Seq[(String, String, String)]] = {
    getAverageLogic(cols) ++ getDiscountPercents(cols) ++ getCashbackPercents(cols) ++ getTravelAverageLogic(cols)
  }

  def getPreferredLogic(colsRU: Seq[String], colsBK: Seq[String]): Seq[(Seq[String], String)] = {
    Seq(
      (Seq("RU_MB_prepaid_total_transaction_count", "RU_MB_postpaid_total_transaction_count"), "RU_MB_preferable_plan_payment_type"),
      (colsRU.map(_ + "_total_transaction_count"), "RU_preferable_transaction_type_by_count"),
      (colsRU.map(_ + "_total_transaction_size"), "RU_preferable_transaction_type_by_size"),
      (colsRU.map(_ + "_first_transaction_date"), "RU_first_transaction_type"),
      (colsRU.map(_ + "_last_transaction_date"), "RU_last_transaction_type"),
      (colsBK.map(_ + "_total_transaction_count"), "BK_preferable_transaction_type_by_count"),
      (colsBK.map(_ + "_total_transaction_size"), "BK_preferable_transaction_type_by_size"),
      (colsBK.map(_ + "_first_transaction_date"), "BK_first_transaction_type"),
      (colsBK.map(_ + "_last_transaction_date"), "BK_last_transaction_type")
    )
  }

  def getPreferred(cols: Seq[String], alias: String, isMax: Boolean = true): Column = {
    val structs = cols.map {
      c =>
        //TODO: Add a mapping of acronymns to actual words to have more readable names

        // Get first three words from column name
        val (l1Cat, l2Cat, thirdWord) = (c.split("_")(0), c.split("_")(1), c.split("_")(2))

        // If second word is a category abbrv, use it otherwise use just first
        val levelPrefix = if (l2Cat matches "[A-Z].*") l1Cat + "_" + l2Cat else l1Cat
        // Exception for preferred payment type feature
        val fillColumnWord = if (alias.contains("preferable_plan_payment_type")) thirdWord else levelPrefix

        when(col(c).isNotNull, struct(col(c).as("v"), lit(fillColumnWord).as("k"))).otherwise(null)
    }
    (if (isMax) greatest(structs: _*) else least(structs: _*)) getItem "k" as alias
  }

  val getMaxOperatorUDF: UserDefinedFunction = udf[String, Seq[Seq[Row]]] { x =>
    val allOpGrouped = x.flatten.map {
      x => (x.getAs[String]("Operator"), x.getAs[Long]("count"))
    }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
    if (allOpGrouped.isEmpty) { "" }
    else { allOpGrouped.maxBy(_._2)._1 }
  }

  val flattenOperatorUDF: UserDefinedFunction = udf[Seq[String], Seq[Seq[Row]]] { x =>
    val allOpGrouped = x.flatten.map {
      x => x.getAs[String]("Operator")
    }.distinct.sorted

    if (allOpGrouped.isEmpty) null else allOpGrouped
  }

  val flattenDueDateSeqUDF: UserDefinedFunction = udf { (arr: Seq[Row]) =>
    val dueDatesArr = arr.map {
      case Row(operator: String, date: Timestamp, amount: Double) => OperatorDueDate(operator, date, amount)
    }
    if (dueDatesArr.isEmpty) null else dueDatesArr
  }

  def getMaxOperator(cols: Seq[String]): Seq[Column] = {
    val opColString = "_operators"
    val opCols = cols.filter(_.contains(opColString))
    opCols.map { x =>
      {
        val colPrefix = x match {
          case s if x.endsWith("_operators")                            => "_operators"
          case s if x.endsWith("_operators_bycount")                    => "_operators_bycount"
          case s if x.endsWith("_operators_bysize")                     => "_operators_bysize"
          case s if x.endsWith("_operators_transaction_bank")           => "_operators_transaction_bank"
          case s if x.endsWith("_operators_transaction_payment_method") => "_operators_transaction_payment_method"
          case s if x.endsWith("_operators_transaction_platform")       => "_operators_transaction_platform"
          case s if x.endsWith("_operators_boardingstation")            => "_operators_boardingstation"
          case s if x.endsWith("_operators_destinationstation")         => "_operators_destinationstation"
          case s if x.endsWith("_operators_class")                      => "_operators_class"
          case s if x.endsWith("_operators_source")                     => "_operators_source"
          case s if x.endsWith("_operators_destination")                => "_operators_destination"
          case s if x.endsWith("_operators_booking_type")               => "_operators_booking_type"
          case s if x.endsWith("_operators_boardingpoint")              => "_operators_boardingpoint"
        }

        val levelPrefix = x.replace(colPrefix, "")

        val newColSuffix = colPrefix match {
          case "_operators" => "transaction_operator"
          case "_operators_bycount" | "_operators_bysize" => "transaction_operator_" + colPrefix.split("_operators_").last
          case _ => colPrefix.split("_operators_").last
        }
        when(col(x).isNotNull, getMaxOperatorUDF(col(x))) as s"${levelPrefix}_preferable_${newColSuffix}"
      }
    }
  }

  val arrayColsToFlatten = Seq(
    // (string to search with, udf to flatten it, alias to rename)
    // Note: keep alias different from searchString, otherwise duplicate columns are created
    ("_IDs", flattenOperatorUDF, "_all_transaction_ids")
  )

  def flattenArrayCols(cols: Seq[String]): Seq[Column] = {
    arrayColsToFlatten.flatMap {
      case (searchStr: String, flattenUDF: UserDefinedFunction, alias: String) =>
        val filteredCols = cols.filter(_.matches(".*" + searchStr))
        filteredCols.map { colName =>
          val levelPrefix = colName.replace(searchStr, "")
          when(col(colName).isNotNull, flattenUDF(col(colName))) as s"$levelPrefix$alias"
        }
    }
  }

  def getDropCols(allCols: Seq[String]): Seq[String] = {
    val extraArrayCols = arrayColsToFlatten.map(_._1)
    allCols.filter(colName =>
      colName.contains("_operators")
        || extraArrayCols.map(c => colName.matches(".*" + c)).reduce(_ || _)
        || colName.contains("_total_distance_per_trip")
        || colName.contains("_total_daystojourney")
        || colName.contains("_has_infant"))
  }

  def getGADropCols(allCols: Seq[String]): Seq[String] = {
    allCols.filter(colName =>
      colName.contains("_clp_sessions_app")
        || colName.contains("_clp_sessions_web")
        || colName.contains("_glp_sessions_app")
        || colName.contains("_glp_sessions_web")
        || colName.contains("_pdp_sessions_app")
        || colName.contains("_pdp_sessions_web")
        || colName.contains("_product_clicks_app")
        || colName.contains("_product_clicks_web")
        || colName.contains("_product_views_app")
        || colName.contains("_product_views_web"))

  }

  val standardizeLanguage = udf[String, String] {
    langString =>
      if (langSets.contains(langString.toLowerCase.trim))
        langString.toLowerCase
      else
        null
  }

  val standardizeCity: UserDefinedFunction = udf[String, String] {
    cityString =>
      val formattedCity = cityString.trim.toLowerCase
      val DelhiPattern = "(.*\\s*delhi\\s*.*)".r
      val NoidaPattern = "(.*\\s*noida\\s*.*)".r

      formattedCity match {
        // Merge New Delhi and Delhi, Greater Noida and Noida
        case DelhiPattern(del) => "New Delhi"
        case NoidaPattern(noi) => "Noida"
        case other             => other.split(' ').map(_.capitalize).mkString(" ")
      }
  }
  val standardizeState: UserDefinedFunction = udf[String, String] {
    stateString => stateString.trim.toLowerCase.split(' ').map(_.capitalize).mkString(" ")
  }
  val standardizeCountry: UserDefinedFunction = udf[String, String] {
    countryString =>
      countryString.toLowerCase.trim match {
        case "india" => "India"
        case _       => null
      }
  }

  val standarizePIN = (column: Column) => {
    when(length(column).equalTo(6) and isAllNumeric(column), column).otherwise(null)
  }

  def standardVersionRegex(versionString: String): String = {
    val versionPattern = "((\\d)|[\\d\\.]+)"
    if (versionString.matches(versionPattern)) versionString
    else "unknown" // keep gibberish versions as unknown so we don't lose osName info
  }

  val standarizeVersion: UserDefinedFunction = udf[String, String] {
    versionString =>
      standardVersionRegex(versionString)
  }

  // UDFs for device OS feature
  val toDeviceOS: UserDefinedFunction = udf((os: String, version: String) => DeviceOS(os, version))
  val flattenDeviceOS: UserDefinedFunction = udf[Seq[DeviceOS], Seq[Seq[Row]]]({
    seqOfSeqX =>
      val allOS = seqOfSeqX.flatten.map {
        case Row(osName: String, osVer: String) => DeviceOS(osName, osVer.toString)
        case Row(osName: String, _: Any)        => DeviceOS(osName, null)
        case _                                  => null
      }.distinct.filter(_ != null) // this will drop cases when osName is not available but will keep unknown version
      if (allOS.isEmpty) null else allOS
  })

  val flattenVersion: UserDefinedFunction = udf[Seq[String], Seq[Seq[String]]]({
    seqOfSeq =>
      val allVersion = seqOfSeq.flatten.map {
        case version: String => standardVersionRegex(version)
        case _               => null
      }.distinct.filter(version => version != null && version != "unknown") // remove unknown versions
      if (allVersion.isEmpty) null else allVersion
  })

  val array_max = udf[String, Seq[String]] { arr => arr.max }

}

