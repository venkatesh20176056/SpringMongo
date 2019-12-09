package com.paytm.map.features.base

import java.lang.Double
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter

object BaseTableUtils {

  def dayIterator(start: DateTime, end: DateTime, formatter: DateTimeFormatter): Seq[String] = Iterator.iterate(start)(_.plusDays(1)).takeWhile(x => x.isEqual(end) || x.isBefore(end)).map(_.toString(formatter)).toSeq

  trait BinaryOperatorCnt[A <: Product]

  implicit object appCtgCnt extends BinaryOperatorCnt[AppCategoryCnt]

  implicit object optCnt extends BinaryOperatorCnt[OperatorCnt]

  case class AppCategoryCnt(category: String, app_count: Long)
  case class OperatorCnt(Operator: String, count: Long)
  case class OperatorDueDate(Operator: String, DueDate: Timestamp, BillAmount: Double)
  case class PromoUsageBase(Promocode: String, Date: String)
  case class PromoUsage(Promocode: String, Date: Timestamp)
  case class LMSAccount(AccountNumber: String, Status: Int, TotalAmount: Double, BalanceAmount: Double, DueDate: Timestamp, LateFee: Double)
  case class LMSAccountV2(AccountNumber: String, TotalAmount: Double, BalanceAmount: Double, DueDate: Timestamp, LateFee: Double, ProductSource: String, ProductType: String, Status: Int, DPD: Int)
  case class SMS(SenderName: String, SentTimestamp: Timestamp)
  case class BankCard(card_type: String, issuer_bank: String, network: String)
  case class TravelDetail(destination_city: String, travel_date: Timestamp)
  case class CinemaVisited(cinema_id: String, transaction_date: Timestamp)
  case class CineplexVisited(dmid: String, transaction_date: Timestamp)
  case class MoviePass(promocode: String, pass_name: String, valid_upto: Timestamp)
  case class CategorizedTxns(category: Option[String], subCategory: Option[String], count: Option[Long], amount: Option[Double])
  case class SuperCashBackCampaign(
    campaignName: String,
    current_unAck_count: Long,
    current_initialized_count: Long,
    current_inprogress_count: Long,
    current_completed_count: Long,
    current_expired_count: Long,
    total_instance_count: Long,
    last_camp_completed_date: Date,
    last_camp_expired_date: Date,
    current_init_inprog_start_date: Date,
    current_init_inprog_expiry_date: Date,
    current_inprogress_stage: Long,
    current_inprog_last_txn_date: Date
  )
  case class SuperCashBackUnAck(
    campaignName: String,
    current_unAck_count: Long
  )
  case class SuperCashBackInitialized(
    campaignName: String,
    current_initialized_count: Long
  )

  case class SuperCashBackInprogress(
    campaignName: String,
    current_inprogress_count: Long
  )
  case class SuperCashBackCompleted(
    campaignName: String,
    current_completed_count: Long
  )

  case class SuperCashBackExpired(
    campaignName: String,
    current_expired_count: Long
  )

  case class SuperCashBackTotalInstance(
    campaignName: String,
    total_instance_count: Long
  )

  case class SuperCashBackLastCampCompleted(
    campaignName: String,
    last_camp_completed_date: Date
  )

  case class SuperCashBackLastCampExpired(
    campaignName: String,
    last_camp_expired_date: Date
  )

  case class SuperCashBackCurrentInProgStart(
    campaignName: String,
    current_init_inprog_start_date: Date
  )

  case class SuperCashBackCurrentInProgExpiry(
    campaignName: String,
    current_init_inprog_expiry_date: Date
  )

  case class SuperCashBackCurrentInProgStage(
    campaignName: String,
    current_inprogress_stage: Long
  )

  case class SuperCashBackCurrentInProgLastTxn(
    campaignName: String,
    current_inprog_last_txn_date: Date
  )

  case class SubWalletLastBalanceCreditedDt(
    ppi_type: String,
    issuer_id: Long,
    last_balance_credited_dt: Date
  )

  case class SubWalletBalance(
    ppi_type: String,
    issuer_id: Long,
    balance: Double
  )
  case class SubWalletBalanceWithDt(
    ppi_type: String,
    issuer_id: Long,
    balance: Double,
    dt: Date
  )
  case class MerchantSuperCashStatus(
    campaign_id: Option[Long],
    last_updated_status: Option[String],
    successful_transaction_count: Option[Long]
  )
  case class MerchantEDCRejectedReason(
    rejected_field_reason: String,
    doc_type: String
  )
  case class ExternalClientSignupDate(
    sign_up_date: Date,
    external_client_id: String
  )

  def columnSum(colList: Seq[Column]): Column = colList.map(colM => coalesce(colM, lit(0))).reduce(_ + _)
  def getMaxColumn(colList: Seq[Column]): Column = colList.reduce((x, y) => when(x >= y, x).otherwise(y))
  def getMaxColumnOrdered(colList: Seq[(Column, Column)]): Column = {
    colList
      .map(colM => (colM._1, coalesce(colM._2, lit(0))))
      .reduce((x, y) => (when(x._2 >= y._2, x._1).otherwise(y._1), when(x._2 >= y._2, x._2).otherwise(y._2)))
      ._1
  }
}
