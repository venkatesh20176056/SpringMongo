package com.paytm.map.features.sanity

import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.utils.Percent
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object SanityConstants {

  val walletLevelPrefix: String = wallet.toUpperCase

  val noPreferableOperatorBySizeVerticals: Seq[String] =
    l3RU ++ l3BK ++ l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(l2BK, walletLevelPrefix)

  val noPreferableOperatorByCountVerticals: Seq[String] =
    l3RU ++ l3BK ++ l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(l2BK, walletLevelPrefix)

  val noDiscountCountVerticals: Seq[String] =
    Seq("RU_CBL", "RU_FT") ++ languageVerticals ++ Seq(walletLevelPrefix)

  val noCashbackCountVerticals: Seq[String] =
    Seq("RU_CBL", "RU_FT", "RU_MB_prepaid", "RU_MB_postpaid", "RU_MB") ++
      languageVerticals ++ l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(walletLevelPrefix)

  val noDiscountSizeVerticals: Seq[String] =
    Seq("RU_CBL", "RU_FT") ++ languageVerticals ++ Seq(walletLevelPrefix)

  val noCashbackSizeVerticals: Seq[String] =
    Seq("RU_CBL", "RU_FT", "RU_MB_prepaid", "RU_MB_postpaid", "RU_MB") ++
      languageVerticals ++ l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(walletLevelPrefix)

  val noFirstPrefOperatorVerticals: Seq[String] =
    l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq("BK", walletLevelPrefix)

  val noFirstLastOperatorVerticals: Seq[String] =
    l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq("BK", walletLevelPrefix)

  val noFirstLastTxDateVerticals = Seq(walletLevelPrefix)

  val noPercentageDiscntByCountVerticals = Seq(walletLevelPrefix)

  val noPercentageCshbkBySizeVerticals: Seq[String] =
    l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(walletLevelPrefix)

  val noPercentageCshbkByCountVerticals: Seq[String] =
    l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(walletLevelPrefix)

  val noGAViewsClicksVerticals: Seq[String] =
    l3RU ++ Seq("BK_TR", "BK_IFL", "BK_AM", walletLevelPrefix) ++
      Seq("EC1_OHNK", "EC1_OC", "EC1_OGNS", "EC1_ANP", "EC1_BNPC", "EC1_GIFTS", "EC1_IB")

  val noPercentageDiscntBySizeVerticals = Seq(walletLevelPrefix)

  val noRatioSumToOneVerticals: Seq[String] =
    l3RU ++ l3BK ++ l3EC1 ++ l3EC2_I ++ l3EC2_II ++ Seq(l2BK)

  def getAllTestAgg(levelPrefix: String): Seq[TestFeat] = {

    //Count Consistency Cheks
    val countConsistencyDiscountCheck =
      if (noDiscountCountVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "CountConsistencyDiscount",
          s"${levelPrefix}_CC_Dscnt",
          "ltEq",
          Seq(s"${levelPrefix}_total_discount_transaction_count", s"${levelPrefix}_total_transaction_count")
        ))
      }

    val countConsistencyCashbackCheck =
      if (noCashbackCountVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "CountConsistencyCashback",
          s"${levelPrefix}_CC_Cshbk",
          "ltEq",
          Seq(s"${levelPrefix}_total_cashback_transaction_count", s"${levelPrefix}_total_transaction_count")
        ))
      }

    val sizeConsistencyDiscountCheck =
      if (noDiscountSizeVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "SizeConsistencyDiscount",
          s"${levelPrefix}_SC_Dscnt",
          "ltEq",
          Seq(s"${levelPrefix}_total_discount_transaction_size", s"${levelPrefix}_total_transaction_size")
        ))
      }

    val sizeConsistencyCashbackCheck =
      if (noCashbackSizeVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "SizeConsistencyCashback",
          s"${levelPrefix}_SC_Cshbk",
          "ltEq",
          Seq(s"${levelPrefix}_total_cashback_transaction_size", s"${levelPrefix}_total_transaction_size"),
          Percent(5)
        ))
      }

    val operatorConsistencyFirstLastCheck =
      if (noFirstLastOperatorVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "OperatorConsistencyFirstLast",
          s"${levelPrefix}_OC_FstLst",
          "isNotNullVV",
          Seq(s"${levelPrefix}_first_transaction_operator", s"${levelPrefix}_last_transaction_operator")
        ))
      }

    val operatorConsistencyPrefOperatorCheck =
      if (noFirstPrefOperatorVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "OperatorConsistencyPrefOperator",
          s"${levelPrefix}_OC_PrfOp",
          "isNotNullVV",
          Seq(s"${levelPrefix}_first_transaction_operator", s"${levelPrefix}_preferable_transaction_operator")
        ))
      }

    val operatorConsistencyPrefOperatorBySizeCheck =
      if (noPreferableOperatorBySizeVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "OperatorConsistencyPrefOperatorBySize",
          s"${levelPrefix}_OC_PrfOpSz",
          "isNotNullVV",
          Seq(s"${levelPrefix}_first_transaction_operator", s"${levelPrefix}_preferable_transaction_operator_bysize")
        ))
      }

    val operatorConsistencyPrefOperatorByCountCheck =
      if (noPreferableOperatorByCountVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "OperatorConsistencyPrefOperatorByCount",
          s"${levelPrefix}_OC_PrfOpCnt",
          "isNotNullVV",
          Seq(s"${levelPrefix}_first_transaction_operator", s"${levelPrefix}_preferable_transaction_operator_bycount")
        ))
      }

    val dateNullityConsistencyFirstLastCheck =
      if (noFirstLastTxDateVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "DateNullityConsistencyFirstLast",
          s"${levelPrefix}_DNC_FstLst",
          "isNotNullVV",
          Seq(s"${levelPrefix}_first_transaction_date", s"${levelPrefix}_last_transaction_date")
        ))
      }

    val dateComparisonConsistencyFirstLastCheck =
      if (noFirstLastTxDateVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "DateComparisonConsistencyFirstLast",
          s"${levelPrefix}_DCC_FstLst",
          "ltEq",
          Seq(s"${levelPrefix}_first_transaction_date", s"${levelPrefix}_last_transaction_date")
        ))
      }

    val dateComparisonConsistencyLastTodayCheck =
      if (noFirstLastTxDateVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "DateComparisonConsistencyLastToday",
          s"${levelPrefix}_DCC_LstTdy",
          "ltEq",
          Seq(s"${levelPrefix}_last_transaction_date", s"today_date")
        ))
      }

    val dateComparisonConsistencyDefaultFirstCheck =
      if (noFirstLastTxDateVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "DateComparisonConsistencyDefaultFirst",
          s"${levelPrefix}_DCC_DefFst",
          "ltEq",
          Seq(s"default_min_date", s"${levelPrefix}_first_transaction_date")
        ))
      }

    val valueConsistencyPercentageDiscntBySizeCheck =
      if (noPercentageDiscntBySizeVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "ValueConsistencyPercentageDiscntBySize",
          s"${levelPrefix}_VC_PctDBS",
          "between",
          Seq(s"${levelPrefix}_percentage_discount_by_transaction_size", s"0,1")
        ))
      }

    val valueConsistencyPercentageDiscntByCountCheck =
      if (noPercentageDiscntByCountVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "ValueConsistencyPercentageDiscntByCount",
          s"${levelPrefix}_VC_PctDBC",
          "between",
          Seq(s"${levelPrefix}_percentage_discount_by_transaction_count", s"0,1")
        ))
      }

    val valueConsistencyPercentageCashbackBySizeCheck =
      if (noPercentageCshbkBySizeVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "ValueConsistencyPercentageCashbackBySize",
          s"${levelPrefix}_VC_PctCBS",
          "between",
          Seq(s"${levelPrefix}_percentage_cashback_by_transaction_size", s"0,1")
        ))
      }

    val valueConsistencyPercentageCashbackByCountCheck =
      if (noPercentageCshbkByCountVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "ValueConsistencyPercentageCashbackByCount",
          s"${levelPrefix}_VC_PctCBC",
          "between",
          Seq(s"${levelPrefix}_percentage_cashback_by_transaction_count", s"0,1")
        ))
      }

    val behavorialFeatConsistencyAppCheck =
      if (noGAViewsClicksVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "BehavorialFeatConsistencyApp",
          s"${levelPrefix}_BFC_App",
          "ltEq",
          Seq(s"${levelPrefix}_total_product_clicks_app", s"${levelPrefix}_total_product_views_app")
        ))
      }

    val behavorialFeatConsistencyWebCheck =
      if (noGAViewsClicksVerticals.contains(levelPrefix)) Seq()
      else {
        Seq(TestFeat(
          "BehavorialFeatConsistencyWeb",
          s"${levelPrefix}_BFC_Web",
          "ltEq",
          Seq(s"${levelPrefix}_total_product_clicks_web", s"${levelPrefix}_total_product_views_web")
        ))
      }

    val ratioSumToOneCheck = if (noRatioSumToOneVerticals.contains(levelPrefix)) Seq()
    else {
      Seq(TestFeat(
        "A2WPaymentRatioSumToOneCheck",
        s"${levelPrefix}_A2W_RSO",
        "sumLtEqOne",
        Seq(
          s"${levelPrefix}_A2W_CC_ratio",
          s"${levelPrefix}_A2W_DC_ratio",
          s"${levelPrefix}_A2W_NB_ratio",
          s"${levelPrefix}_A2W_NEFT_ratio",
          s"${levelPrefix}_A2W_UPI_ratio"
        )
      ))
    }

    //TODO: Add more consistency checks

    countConsistencyDiscountCheck ++
      countConsistencyCashbackCheck ++
      sizeConsistencyDiscountCheck ++
      sizeConsistencyCashbackCheck ++
      operatorConsistencyFirstLastCheck ++
      operatorConsistencyPrefOperatorCheck ++
      operatorConsistencyPrefOperatorBySizeCheck ++
      operatorConsistencyPrefOperatorByCountCheck ++
      dateNullityConsistencyFirstLastCheck ++
      dateComparisonConsistencyFirstLastCheck ++
      dateComparisonConsistencyLastTodayCheck ++
      dateComparisonConsistencyDefaultFirstCheck ++
      //valueConsistencyPercentageDiscntBySizeCheck ++
      //valueConsistencyPercentageDiscntByCountCheck ++
      //valueConsistencyPercentageCashbackBySizeCheck ++
      //valueConsistencyPercentageDiscntByCountCheck ++
      behavorialFeatConsistencyAppCheck ++
      behavorialFeatConsistencyWebCheck ++
      ratioSumToOneCheck
  }

  def getAllTestAggAll(verticals: Seq[String]): Seq[TestFeat] = {
    verticals.flatMap(getAllTestAgg)
  }

  val getState: UserDefinedFunction = udf { (passedCounts: Long, failedCounts: Long, tolerance: Double) =>
    val totalCounts = passedCounts + failedCounts
    if (failedCounts / totalCounts.toDouble > tolerance) "Failed" else "Passed"
  }

}
