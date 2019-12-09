

package com.paytm.map.features.Chakki

import com.paytm.map.features.Chakki.FeatChakki.Constants.dId2DataType
import com.paytm.map.features.Chakki.FeatChakki._
import com.paytm.map.features.Chakki.Features.TimeSeries.{timeSeries}
import org.apache.spark.sql.Row

package object Features {

  /**
   * This Function is to map the specification Sheet to a feature object
   *
   * @param varName    : varName of the feature to be needed.
   * @param tsSet      : timeSeries for which the feature object is needed.
   * @param row        : Row object containing feature specs
   * @param isFinalSet : Time series for which feature would be exposed
   * @return : feature obect as per specification
   */
  def row2Feat(varName: String, tsSet: Seq[String], row: Row, isFinalSet: Seq[String] = null): Feat = {

    // Mandatory Attributes
    val ts = tsSet.map(x => timeSeries(x.trim))
    val isFinal = isFinalSet.filter(_ != "").map(x => timeSeries(x.trim))
    val dataType = dId2DataType(row.getAs[String]("dType"))
    val function = row.getAs[String]("fun")

    // Options Attributes
    //optional defined for stage
    def stage: UDF_T = if (row.getAs[String]("stage") == "POST_UDF") POST_UDF else PRE_UDF

    def baseColumn: String = {
      val baseCol = row.getAs[String]("baseColumn")
      if (baseCol != null) baseCol else ""
    }

    def partitionBy = {
      val partn = row.getAs[String]("partitionBy")
      if (partn != null) partn else "customer_id"
    }

    def orderBy = {
      val orderCol = row.getAs[String]("orderBy")
      if (orderCol != null) orderCol else "dt"
    }

    //NESTED\nGET_PERCENTBY\nGET_PREF """
    function.toUpperCase match {
      //UDF Features
      case "DIVSN" => DivsnFeatUDF(varName, ts, dataType, stage, row.getAs[String]("downCol"), baseColumn, isFinal = isFinal)
      case "PACK2MAP" =>
        val pattern = row.getAs[String]("pattern")
        val catList = row.getAs[String]("catList")
          .split(",")
          .map(_.split("|"))
          .map(x => (x(0), x(1)))
        Pack2MapFeat(varName, ts, dataType, stage, pattern, catList, isFinal)
      case "LISTSIZE" => ListSizeFeat(varName, ts, dataType, stage, baseColumn, isFinal = isFinal)
      case "ADD"      => AddFeat(varName, ts, dataType, stage, baseColumn, isFinal = isFinal)
      case "GETPREFBY" => {
        val pattern = row.getAs[String]("pattern")
        val prefixList = row.getAs[String]("prefixList").split(",")
        val isMax = row.getAs[Any]("isMax").toString.toInt // Avoid Spark Bug
        GetPrefByFeat(varName, ts, dataType, stage, isMax, pattern, prefixList, isFinal)
      }
      case "WEEK-DAY"       => WeekDayFeat(varName, ts, dataType, stage, baseColumn, isFinal = isFinal)

      // UDAF Features
      case "MIN"            => MinFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "MAX"            => MaxFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "SUM"            => SumFeat(varName, ts, dataType, baseColumn = baseColumn, isFinal = isFinal)
      case "MEAN"           => MeanFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "COUNT"          => CountFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "COLLECT_UNIK"   => CollectUnikFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "COLLECT_LIST"   => CollectListFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "COLLECT_SET"    => CollectSetFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "COUNT_DISTINCT" => CountDistinctFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "PREFTXNDAY" => {
        val byCol = row.getAs[String]("by")
        PrefTxnDayFeat(varName, ts, dataType, byCol, baseColumn, isFinal = isFinal)
      }
      case "MAX_OPERATOR"                => MaxOperatorFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "PROMO_USAGE"                 => PromoUsageFeat(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "SMS_AGG"                     => SMSAgg(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_AGG"           => SuperCashBackAggSingle(varName, ts, dataType, baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_UNACK"         => SuperCashBackAgg(varName, ts, dataType, "unAck_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INITIALIZED"   => SuperCashBackAgg(varName, ts, dataType, "initialized_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INPROGRESS"    => SuperCashBackAgg(varName, ts, dataType, "inprogress_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_COMPLETED"     => SuperCashBackAgg(varName, ts, dataType, "completed_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_EXPIRED"       => SuperCashBackAgg(varName, ts, dataType, "expired_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_TOTALINSTANCE" => SuperCashBackAgg(varName, ts, dataType, "total_instance_count", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_LASTCOMPLETED" => SuperCashBackAgg(varName, ts, dataType, "last_completed_date", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_LASTEXPIRED"   => SuperCashBackAgg(varName, ts, dataType, "last_expired_date", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INPROGSTART"   => SuperCashBackAgg(varName, ts, dataType, "inprog_start_date", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INPROGEXPIRY"  => SuperCashBackAgg(varName, ts, dataType, "inprog_expiry_date", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INPROGSTAGE"   => SuperCashBackAgg(varName, ts, dataType, "inprog_stage", baseColumn, isFinal = isFinal)
      case "SUPERCASHBACK_INRPOGLASTXN"  => SuperCashBackAgg(varName, ts, dataType, "inprog_last_txn_date", baseColumn, isFinal = isFinal)

      case "NESTED_MAX" => {
        val groupByCols: Seq[String] =
          Option(row.getAs[String]("groupBy"))
            .map(_.split(",").toSeq)
            .getOrElse(throw new Exception(s"groupBy field is missing in specsheet for feature $varName"))

        val maxByCol: String =
          Option(row.getAs[String]("maxBy"))
            .getOrElse(throw new Exception(s"maxBy field is missing in specsheet for column $varName"))

        NestedMaxFeat(varName, ts, dataType, groupByCols, maxByCol, baseColumn, isFinal = isFinal)
      }

      case "NESTED_SUM" => {
        val groupByCols: Seq[String] =
          Option(row.getAs[String]("groupBy"))
            .map(_.split(",").toSeq)
            .getOrElse(throw new Exception(s"groupBy field is missing in specsheet for feature $varName"))

        val sumOverCols: Seq[String] =
          Option(row.getAs[String]("sumOver"))
            .map(_.split(",").toSeq)
            .getOrElse(throw new Exception(s"sumOver field is missing in specsheet for column $varName"))

        NestedSumByFeat(varName, ts, dataType, groupByCols, sumOverCols, baseColumn, isFinal = isFinal)
      }

      case "POSTPAID_TOP_MERCHANT" => {
        val groupByCols: Seq[String] =
          Option(row.getAs[String]("groupBy"))
            .map(_.split(",").toSeq)
            .getOrElse(throw new Exception(s"groupBy field is missing in specsheet for feature $varName"))

        val maxByCol: String =
          Option(row.getAs[String]("maxBy"))
            .getOrElse(throw new Exception(s"maxBy field is missing in specsheet for column $varName"))

        PostPaidTopMerchantFeat(varName, ts, dataType, groupByCols, maxByCol, baseColumn, isFinal = isFinal)
      }

      // UDWF Features
      case "GETFIRST"                          => GetFirstFeat(varName, ts, dataType, partitionBy, orderBy, baseColumn, isFinal)
      case "GETLAST"                           => GetLastFeat(varName, ts, dataType, partitionBy, orderBy, baseColumn, isFinal)

      //SubWallet Features
      case "SUBWALLET_LASTBALANCECREDITEDDATE" => SubWalletAgg(varName, ts, dataType, "last_balance_credited_dt", baseColumn, isFinal = isFinal)
      case "SUBWALLET_BALANCE"                 => SubWalletAgg(varName, ts, dataType, "subwallet_balance", baseColumn, isFinal = isFinal)

      //Geohash features
      case "GEOHASH_PREFIX"                    => GeohashPrefixFeat(varName, ts, dataType, stage, baseColumn, isFinal)
      case "MAX_GEOHASH_FILTERED"              => MaxGeohashFilteredFeat(varName, ts, dataType, baseColumn, isFinal)
    }
  }
}
