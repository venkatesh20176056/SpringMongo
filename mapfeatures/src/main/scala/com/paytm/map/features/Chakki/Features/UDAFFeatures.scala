package com.paytm.map.features.Chakki.Features

import java.sql.{Date, Timestamp}

import com.paytm.map.features.Chakki.Features.TimeSeries.tsType
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.utils.UDFs.flattenSeqOfSeq
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//********************************* UDAF Features ***********************************
sealed trait UDAFFeat extends Feat {
  //defined members
  val dependency = Seq(getBaseColumn)
}

case class SumFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = sum(getBaseColumn)
}

case class MinFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = min(getBaseColumn)
}

case class MaxFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = max(getBaseColumn)
}

case class MeanFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = mean(getBaseColumn)
}

case class CountFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = count(getBaseColumn)
}

case class CollectUnikFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val collectUniq: UserDefinedFunction = dataType match {
    case ArrayType(StringType, true) => udf[Seq[String], Seq[Seq[String]]]((seqOfSeqX: Seq[Seq[String]]) => seqOfSeqX.flatten.distinct)
    case _                           => udf({ seqOfSeqX: Seq[Seq[Any]] => seqOfSeqX.flatten.distinct }, dataType)
  }
  val outCol: Column = collectUniq(collect_set(getBaseColumn))
}

case class CollectListFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = collect_list(getBaseColumn)
}

case class CollectSetFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = collect_set(getBaseColumn)
}

case class CountDistinctFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val outCol: Column = approx_count_distinct(getBaseColumn)
}

case class MaxOperatorFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val getMaxOperator: UserDefinedFunction = udf[String, Seq[Seq[Row]]] { x =>
    val allOpGrouped = x.flatten.map {
      x => (x.getAs[String]("Operator"), x.getAs[Long]("count"))
    }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
    if (allOpGrouped.isEmpty) { "" }
    else { allOpGrouped.maxBy(_._2)._1 }
  }

  val outCol: Column = getMaxOperator(collect_list(getBaseColumn))
}

case class PrefTxnDayFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, byCol: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {

  override val dependency: Seq[String] = Seq(getBaseColumn, byCol)

  val getPrefDayUDF: UserDefinedFunction = udf[String, Seq[Row]] { x =>
    val allGrouped = x
      .map(row => (row.getAs[String]("k"), row.getAs[Long]("v")))
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)

    if (allGrouped.isEmpty) { "" }
    else { allGrouped.maxBy(_._2)._1 }
  }

  val newStructCol: Column = collect_list(struct(col(byCol).as("v"), col(getBaseColumn).as("k")))
  val outCol: Column = getPrefDayUDF(newStructCol)
}

case class PromoUsageFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val getPromoUsageUDF: UserDefinedFunction = udf[Seq[PromoUsageBase], Seq[Seq[Row]]]((seqOfSeqPromo: Seq[Seq[Row]]) => {
    val allCodes = seqOfSeqPromo.flatten.map {
      case Row(operator: String, date: String) => PromoUsageBase(operator, date)
    }.distinct
    if (allCodes.isEmpty) null else allCodes
  })
  val outCol: Column = getPromoUsageUDF(collect_set(getBaseColumn))
}

case class SMSAgg(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  val getMaxSenderUDF: UserDefinedFunction = udf[Seq[SMS], Seq[Seq[Row]]] { x =>
    val allSenderGrouped = x.flatten.map {
      x => (x.getAs[String]("sender_name"), x.getAs[Timestamp]("sent_timestamp"))
    }
      .groupBy(_._1)
      .mapValues(_.map(_._2).maxBy(_.getTime))
      .map {
        case (sender: String, max_timestamp: Timestamp) => SMS(sender.toLowerCase, max_timestamp)
      }.toSeq

    if (allSenderGrouped.isEmpty) null else allSenderGrouped
  }
  val outCol: Column = getMaxSenderUDF(collect_set(getBaseColumn))
}

case class SuperCashBackAggSingle(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {

  //val getSuperCashStruct: UserDefinedFunction = udf[Seq[SuperCashBackCampaign], Seq[Row]]((inCol: Seq[Row]) => {
  val getSuperCashStruct: UserDefinedFunction = udf[Seq[SuperCashBackCampaign], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCampaign(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_unAck_count"),
      a.getAs[Long]("current_initialized_count"),
      a.getAs[Long]("current_inprogress_count"),
      a.getAs[Long]("current_completed_count"),
      a.getAs[Long]("current_expired_count"),
      a.getAs[Long]("total_instance_count"),
      a.getAs[Date]("last_camp_completed_date"),
      a.getAs[Date]("last_camp_expired_date"),
      a.getAs[Date]("current_init_inprog_start_date"),
      a.getAs[Date]("current_init_inprog_expiry_date"),
      a.getAs[Long]("current_inprogress_stage"),
      a.getAs[Date]("current_inprog_last_txn_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCampaign(
          a.campaignName,
          a.current_unAck_count + b.current_unAck_count,
          a.current_initialized_count + b.current_initialized_count,
          a.current_inprogress_count + b.current_inprogress_count,
          a.current_completed_count + b.current_completed_count,
          a.current_expired_count + b.current_expired_count,
          a.total_instance_count + b.total_instance_count,
          Seq(a.last_camp_completed_date, b.last_camp_completed_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong)),
          Seq(a.last_camp_expired_date, b.last_camp_expired_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong)),
          Seq(a.current_init_inprog_start_date, b.current_init_inprog_start_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong)),
          Seq(a.current_init_inprog_expiry_date, b.current_init_inprog_expiry_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong)),
          a.current_inprogress_stage + b.current_inprogress_stage,
          Seq(a.current_inprog_last_txn_date, b.current_inprog_last_txn_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val outCol: Column = getSuperCashStruct(collect_set(getBaseColumn))

}

case class SuperCashBackAgg(varName: String, timeAgg: Seq[tsType], dataType: DataType, scb_feat: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {

  val getunAckCountStruct: UserDefinedFunction = udf[Seq[SuperCashBackUnAck], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackUnAck(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_unAck_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackUnAck(
          a.campaignName,
          a.current_unAck_count + b.current_unAck_count
        ))).values.toSeq
  }

  val getInitializedStruct: UserDefinedFunction = udf[Seq[SuperCashBackInitialized], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackInitialized(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_initialized_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackInitialized(
          a.campaignName,
          a.current_initialized_count + b.current_initialized_count
        ))).values.toSeq
  }

  val getInProgressStruct: UserDefinedFunction = udf[Seq[SuperCashBackInprogress], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackInprogress(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_inprogress_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackInprogress(
          a.campaignName,
          a.current_inprogress_count + b.current_inprogress_count
        ))).values.toSeq
  }

  val getCompletedStruct: UserDefinedFunction = udf[Seq[SuperCashBackCompleted], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCompleted(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_completed_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCompleted(
          a.campaignName,
          a.current_completed_count + b.current_completed_count
        ))).values.toSeq
  }

  val getExpiredStruct: UserDefinedFunction = udf[Seq[SuperCashBackExpired], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackExpired(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_expired_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackExpired(
          a.campaignName,
          a.current_expired_count + b.current_expired_count
        ))).values.toSeq
  }

  val getTotalInstanceCount: UserDefinedFunction = udf[Seq[SuperCashBackTotalInstance], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackTotalInstance(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("total_instance_count")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackTotalInstance(
          a.campaignName,
          a.total_instance_count + b.total_instance_count
        ))).values.toSeq
  }

  val getLastCompletedStruct: UserDefinedFunction = udf[Seq[SuperCashBackLastCampCompleted], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackLastCampCompleted(
      a.getAs[String]("campaign_name"),
      a.getAs[Date]("last_camp_completed_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackLastCampCompleted(
          a.campaignName,
          Seq(a.last_camp_completed_date, b.last_camp_completed_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val getLastExpiredStruct: UserDefinedFunction = udf[Seq[SuperCashBackLastCampExpired], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackLastCampExpired(
      a.getAs[String]("campaign_name"),
      a.getAs[Date]("last_camp_expired_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackLastCampExpired(
          a.campaignName,
          Seq(a.last_camp_expired_date, b.last_camp_expired_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val getInprogStartDateStruct: UserDefinedFunction = udf[Seq[SuperCashBackCurrentInProgStart], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCurrentInProgStart(
      a.getAs[String]("campaign_name"),
      a.getAs[Date]("last_camp_expired_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCurrentInProgStart(
          a.campaignName,
          Seq(a.current_init_inprog_start_date, b.current_init_inprog_start_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val getInProgExpiryDate: UserDefinedFunction = udf[Seq[SuperCashBackCurrentInProgExpiry], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCurrentInProgExpiry(
      a.getAs[String]("campaign_name"),
      a.getAs[Date]("current_init_inprog_expiry_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCurrentInProgExpiry(
          a.campaignName,
          Seq(a.current_init_inprog_expiry_date, b.current_init_inprog_expiry_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val getInProgessStageStruct: UserDefinedFunction = udf[Seq[SuperCashBackCurrentInProgStage], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCurrentInProgStage(
      a.getAs[String]("campaign_name"),
      a.getAs[Long]("current_inprogress_stage")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCurrentInProgStage(
          a.campaignName,
          Seq(a.current_inprogress_stage, b.current_inprogress_stage).max
        ))).values.toSeq
  }

  val getInProgLastxnDate: UserDefinedFunction = udf[Seq[SuperCashBackCurrentInProgLastTxn], Seq[Row]] { inCol =>
    inCol.map(a => SuperCashBackCurrentInProgLastTxn(
      a.getAs[String]("campaign_name"),
      a.getAs[Date]("current_inprog_last_txn_date")
    ))
      .groupBy(_.campaignName)
      .mapValues(_.reduce((a, b) =>
        SuperCashBackCurrentInProgLastTxn(
          a.campaignName,
          Seq(a.current_inprog_last_txn_date, b.current_inprog_last_txn_date).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val outCol: Column = scb_feat match {
    case "unAck_count"          => getunAckCountStruct(collect_set(getBaseColumn))
    case "initialized_count"    => getInitializedStruct(collect_set(getBaseColumn))
    case "inprogress_count"     => getInProgressStruct(collect_set(getBaseColumn))
    case "completed_count"      => getCompletedStruct(collect_set(getBaseColumn))
    case "expired_count"        => getExpiredStruct(collect_set(getBaseColumn))
    case "total_instance_count" => getTotalInstanceCount(collect_set(getBaseColumn))
    case "last_completed_date"  => getLastCompletedStruct(collect_set(getBaseColumn))
    case "last_expired_date"    => getLastExpiredStruct(collect_set(getBaseColumn))
    case "inprog_start_date"    => getInprogStartDateStruct(collect_set(getBaseColumn))
    case "inprog_expiry_date"   => getInProgExpiryDate(collect_set(getBaseColumn))
    case "inprog_stage"         => getInProgessStageStruct(collect_set(getBaseColumn))
    case "inprog_last_txn_date" => getInProgLastxnDate(collect_set(getBaseColumn))
  }

}

case class SubWalletAgg(varName: String, timeAgg: Seq[tsType], dataType: DataType, subwallet_feat: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {

  val flattenLastBalanceCreditedDateArrayOfArrays = udf((xs: Seq[Seq[SubWalletLastBalanceCreditedDt]]) => xs.flatten)

  val flattenSubWalletBalanceArrayOfArrays = udf((xs: Seq[Seq[SubWalletBalanceWithDt]]) => xs.flatten)

  val getSubWalletLastBalanceCreditedDt: UserDefinedFunction = udf[Seq[SubWalletLastBalanceCreditedDt], Seq[Row]] { inCol =>
    inCol.map(
      a => SubWalletLastBalanceCreditedDt(
        a.getAs[String]("ppi_type"),
        a.getAs[Long]("issuer_id"),
        a.getAs[Date]("last_balance_credited_dt")
      )
    )
      .groupBy(key => (key.ppi_type, key.issuer_id))
      .mapValues(_.reduce((a, b) =>
        SubWalletLastBalanceCreditedDt(
          a.ppi_type,
          a.issuer_id,
          Seq(a.last_balance_credited_dt, b.last_balance_credited_dt).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
  }

  val getSubWalletBalance: UserDefinedFunction = udf[Seq[SubWalletBalance], Seq[Row]] { inCol =>
    inCol.map(
      a => SubWalletBalanceWithDt(
        a.getAs[String]("ppi_type"),
        a.getAs[Long]("issuer_id"),
        a.getAs[Double]("balance"),
        a.getAs[Date]("dt")
      )
    )
      .groupBy(key => (key.ppi_type, key.issuer_id))
      .mapValues(_.reduce((a, b) =>
        SubWalletBalanceWithDt(
          a.ppi_type,
          a.issuer_id,
          if (a.dt.before(b.dt)) b.balance else a.balance,
          Seq(a.dt, b.dt).maxBy(date => Option(date).map(_.getTime).getOrElse(0.toLong))
        ))).values.toSeq
      .map(
        a => SubWalletBalance(
          a.ppi_type,
          a.issuer_id,
          a.balance
        )
      )
  }

  val outCol: Column = subwallet_feat match {
    case "last_balance_credited_dt" =>
      getSubWalletLastBalanceCreditedDt(
        flattenLastBalanceCreditedDateArrayOfArrays(
          collect_set(getBaseColumn)
        )
      )

    case "subwallet_balance" =>
      getSubWalletBalance(
        flattenSubWalletBalanceArrayOfArrays(
          collect_set(getBaseColumn)
        )
      )
  }
}

case class NestedMaxFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, groupByCols: Seq[String], maxByCol: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  /**
   * input: array<struct(a, b, c)>
   * output: array<struct(a, b, c)>, collect_set is applied to input col, then within each set,
   * group by the fields defined in `groupBy` and apply max on remaining fields.
   */
  def groupedMax(groupByCols: Seq[String], maxByColumn: String): UserDefinedFunction = udf({ seqOfseq: Seq[Seq[Row]] =>
    val maxBy = (rows: Seq[Row], maxByColumn: String) => {
      val idx = rows.head.fieldIndex(maxByColumn)
      val dataType = rows.head.schema(idx).dataType
      dataType match {
        case IntegerType   => rows.maxBy(_.getAs[Integer](maxByColumn))
        case LongType      => rows.maxBy(_.getAs[Long](maxByColumn))
        case DoubleType    => rows.maxBy(_.getAs[Double](maxByColumn))
        case TimestampType => rows.maxBy(_.getAs[Timestamp](maxByColumn).getTime)
      }
    }

    val output = seqOfseq.flatten.groupBy { row =>
      groupByCols match {
        case Seq(a)       => (row.getAs[Any](a))
        case Seq(a, b)    => (row.getAs[Any](a), row.getAs[Any](b))
        case Seq(a, b, c) => (row.getAs[Any](a), row.getAs[Any](b), row.getAs[Any](c))
      }
    }
      .mapValues { rows => maxBy(rows, maxByColumn) }
      .map { case (_, row) => row }
      .toSeq

    if (output.isEmpty) null else output
  }, dataType)

  val outCol: Column = groupedMax(groupByCols, maxByCol)(collect_set(getBaseColumn))
}

//TODO can sum multiple columns
case class NestedSumByFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, groupByCols: Seq[String], sumOverCols: Seq[String], baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  /**
   * input: array<struct(a, b)>
   * output: array<struct(a, sum(b))>, collect_list is applied to input col, then within each set,
   * group by the fields defined in `groupBy` and apply sum on sumByCol.
   */
  def groupedSum(groupByCols: Seq[String], sumCols: Seq[String]): UserDefinedFunction = udf({ seqOfseq: Seq[Seq[Row]] =>
    def sumRows(rows: Seq[Row], sumCols: Seq[String]): Seq[Any] = {
      sumCols.map { sumCol =>
        val idx = rows.head.fieldIndex(sumCol)
        val dataType = rows.head.schema(idx).dataType
        dataType match {
          case DoubleType  => rows.map(_.getAs[Double](sumCol)).sum
          case IntegerType => rows.map(_.getAs[Int](sumCol)).sum
          case LongType    => rows.map(_.getAs[Long](sumCol)).sum
          case _           => null
        }
      }
    }

    def getGroup(row: Row): Seq[Any] = groupByCols match {
      case Seq(a)       => Seq(row.getAs[Any](a))
      case Seq(a, b)    => Seq(row.getAs[Any](a), row.getAs[Any](b))
      case Seq(a, b, c) => Seq(row.getAs[Any](a), row.getAs[Any](b), row.getAs[Any](c))
    }

    if (seqOfseq.nonEmpty) {
      seqOfseq
        .flatten
        .groupBy(row => getGroup(row))
        .mapValues { rows => sumRows(rows, sumCols) }
        .map { case (group, sums) => Row.fromSeq(group ++ sums) }
        .toSeq
    } else {
      null
    }

  }, dataType)

  val outCol: Column = groupedSum(groupByCols, sumOverCols)(collect_list(getBaseColumn))
}

case class PostPaidTopMerchantFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, groupByCols: Seq[String], sumByCol: String, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  /**
   * input: array<struct(a, b, c)>
   * output: array<struct(a, b, c)>, collect_set is applied to input col, then within each set,
   * group by the fields defined in `groupBy` and apply max on remaining fields.
   */
  def groupedSum(groupByCols: Seq[String], sumByColumn: String): UserDefinedFunction = udf({ seqOfseq: Seq[Seq[Row]] =>
    def sumBy(rows: Seq[Row], sumByColumn: String): Any = {
      val idx = rows.head.fieldIndex(sumByColumn)
      val dataType = rows.head.schema(idx).dataType
      dataType match {
        case IntegerType => rows.map(_.getAs[Int](sumByColumn)).sum
        case LongType    => rows.map(_.getAs[Long](sumByColumn)).sum
        case DoubleType  => rows.map(_.getAs[Double](sumByColumn)).sum
      }
    }

    if (seqOfseq.nonEmpty) {
      seqOfseq.flatten.groupBy { row =>
        groupByCols match {
          case Seq(a)       => (row.getAs[Any](a))
          case Seq(a, b)    => (row.getAs[Any](a), row.getAs[Any](b))
          case Seq(a, b, c) => (row.getAs[Any](a), row.getAs[Any](b), row.getAs[Any](c))
        }
      }
        .mapValues { rows => sumBy(rows, sumByColumn) }
        .map { case (group, sum) => if (sum == 0) Row(null, 0L) else Row(group, sum) }
        .maxBy(_.getAs[Long](1)).getAs[String](0)
    } else {
      null
    }

  }, dataType)

  val outCol: Column = groupedSum(groupByCols, sumByCol)(collect_set(getBaseColumn))
}

case class MaxGeohashFilteredFeat(varName: String, timeAgg: Seq[tsType], dataType: DataType, baseColumn: String = "", isFinal: Seq[tsType] = null) extends UDAFFeat {
  override val dependency: Seq[String] = getBaseColumn.split(",").map(_.trim)
  val baseCol = dependency.head
  val notInCols = dependency.tail

  val mostFrequentStringNotInUdf: UserDefinedFunction = udf[String, Seq[Seq[Row]], Seq[Seq[Row]], Seq[Seq[Row]]] { (strs, notIn1, notIn2) =>
    val getMaxOperator: Seq[Seq[Row]] => String = { x =>
      if (x == null || x.flatten.isEmpty) null
      else {
        x.flatten
          .map { case Row(operator: String, count: Long) => (operator, count) }
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .maxBy(_._2)
          ._1
      }
    }

    if (strs == null || strs.isEmpty) null
    else {
      val notInStrs = Seq(notIn1, notIn2).map(getMaxOperator)
      val inputStrs = strs
        .map { rows => rows.filterNot { case Row(operator: String, count: Long) => notInStrs.contains(operator) } }
      getMaxOperator(inputStrs)
    }
  }

  val outCol: Column = mostFrequentStringNotInUdf(collect_list(baseCol), collect_list(notInCols(0)), collect_list(notInCols(1)))
}