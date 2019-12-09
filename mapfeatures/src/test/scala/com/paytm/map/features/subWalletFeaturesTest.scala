package com.paytm.map.features

import java.math.BigInteger
import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.paytm.map.features.base.BaseTableUtils._
import org.scalatest.{FunSpec, Matchers}
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

case class test1(
  customer_id: java.lang.Long,
  dt: Date,
  ppi_type: java.lang.String,
  sub_wallet_id: java.lang.Integer,
  issuer_id: java.lang.Long,
  balance: java.lang.Double,
  create_timestamp: Timestamp
)

class subWalletFeaturesTest extends FunSpec with Matchers with DataFrameSuiteBase {

  describe("subwallet udf's and queries in walletFeatures.scala") {

    it("should execute the udf's and queries properly ") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      // Last Upload DT agg

      val inputDF = Seq(
        ("10", "2018-11-06", "Food", 0L, Date.valueOf("2018-01-01")),
        ("10", "2018-11-06", "Food", 1L, Date.valueOf("2018-01-02")),
        ("10", "2018-11-07", "Food", 0L, Date.valueOf("2018-01-03")),
        ("10", "2018-11-07", "Gift", 0L, Date.valueOf("2018-01-03")),
        ("10", "2018-11-07", "Gift", 0L, Date.valueOf("2018-01-04")),
        ("10", "2018-11-07", "Gift", 1L, Date.valueOf("2018-01-01"))
      )
        .toDF("customer_id", "dt", "ppi_type", "issuer_id", "last_balance_credited_dt")

      inputDF.show()
      val aggDF = inputDF
        .withColumn("lastUploadDtStruct", struct(col("ppi_type"), col("issuer_id"), col("last_balance_credited_dt")))
        .groupBy("customer_id", "dt")
        .agg(
          collect_set("lastUploadDtStruct").alias("lastUploadDtStructSet")
        )

      aggDF.printSchema()
      aggDF.show(truncate = false)

      val flatten1 = udf((xs: Seq[Seq[SubWalletLastBalanceCreditedDt]]) => xs.flatten)

      val getSubWalletLastUploadDt: UserDefinedFunction = udf[Seq[SubWalletLastBalanceCreditedDt], Seq[Row]] { inCol =>
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

      val furtherAgg = aggDF
        .groupBy("customer_id")
        .agg(getSubWalletLastUploadDt(
          flatten1(
            collect_set("lastUploadDtStructSet")
          )
        ).alias("lastUploadDtStructSetOfSet"))

      furtherAgg.show(truncate = false)

      // Sub wallet balance agg

      val balanceInputDF = Seq(("10", Date.valueOf("2018-11-06"), "Food", 0L, 1D), ("10", Date.valueOf("2018-11-06"), "Food", 1L, 12D), ("10", Date.valueOf("2018-11-06"), "Gift", 0L, 123.asInstanceOf[Double]), ("10", Date.valueOf("2018-11-07"), "Gift", 0L, 13.asInstanceOf[Double]), ("10", Date.valueOf("2018-11-07"), "Food", 0L, 20.asInstanceOf[Double]), ("10", Date.valueOf("2018-11-07"), "Gift", 1L, 11.asInstanceOf[Double]), ("10", Date.valueOf("2018-11-07"), "Food", 1L, 10.asInstanceOf[Double]), ("11", Date.valueOf("2018-11-06"), "Food", 1L, 10.asInstanceOf[Double]), ("11", Date.valueOf("2018-11-07"), "Food", 1L, 11.asInstanceOf[Double])).toDF("customer_id", "dt", "ppi_type", "issuer_id", "balance")
      balanceInputDF.show(truncate = false)

      val balAggDF = balanceInputDF
        .withColumn("balanceStruct", struct(col("ppi_type"), col("issuer_id"), col("balance"), col("dt")))
        .groupBy("customer_id", "dt")
        .agg(
          collect_set("balanceStruct").alias("balanceStructSet")
        )
      balAggDF.show(truncate = false)

      val flatten = udf((xs: Seq[Seq[SubWalletBalanceWithDt]]) => xs.flatten)

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

      val furtherAgg1 = balAggDF
        .groupBy("customer_id")
        .agg(getSubWalletBalance(
          flatten(
            collect_set("balanceStructSet")
          )
        ).alias("balanceStructSetOfSet"))

      furtherAgg1.show(truncate = false)

      // Creating Base Tables for Last Upload DT

      val ppi_type = when($"txn_type" === 45, "FoodWallet")
        .when($"txn_type" === 46, "GiftWallet")
        .when($"txn_type" === 57, "FuelWallet")
        .when($"txn_type" === 56, "AllowanceWallet")

      val newSTRTable = Seq(("2018-11-06", 45, 1, 10, 0, 10), ("2018-11-06", 45, 1, 10, 0, 1), ("2018-11-06", 45, 1, 10, 0, 2), ("2018-11-06", 45, 1, 10, 1, 15), ("2018-11-06", 45, 1, 10, 1, 2), ("2018-11-06", 45, 1, 10, 1, 3), ("2018-11-07", 46, 1, 11, 1, 2), ("2018-11-07", 46, 1, 11, 1, 1), ("2018-11-07", 46, 1, 11, 1, 0)).toDF("dt", "txn_type", "txn_status", "payee_id", "payer_id", "dl_last_updated")
      newSTRTable.show()

      val corporateMerchant = Seq((0), (0), (1), (2)).toDF("mid")

      val intermediate = newSTRTable.as('newstr)
        .where($"txn_type".isin(45, 46, 56, 57))
        .where($"txn_status" === 1)
        .join(corporateMerchant.select($"mid").distinct().as('corpmerch), $"newstr.payer_id" === $"corpmerch.mid")
        .withColumn("ppi_type", ppi_type)
        .select($"payee_id".alias("customer_id"), $"dt", $"ppi_type", $"payer_id".alias("issuer_id"), $"dl_last_updated")
        .groupBy("customer_id", "dt", "ppi_type", "issuer_id")
        .agg(max("dl_last_updated").alias("last_upload_dt"))

      intermediate.show()

      // join with columns with original names (without using alias) fails -> tested
      val df1 = Seq((1, 0), (2, 1)).toDF("payer_id", "payee_id")
      df1.show()

      val df22 = df1.select($"payer_id".as("customer_id"), $"payee_id")
      df22.show()

      val df33 = df22.select($"payee_id".as("customer_id"), $"customer_id".as("issuer_id"))
      df33.show()

      // Sub Wallet Bal

      val PPItypeByppitype = when($"ppi_type" === 2, "FoodWallet")
        .when($"ppi_type" === 3, "GiftWallet")
        .when($"ppi_type" === 9, "FuelWallet")
        .when($"ppi_type" === 8, "AllowanceWallet")

      val walletUser = Seq((10.asInstanceOf[Long], "1", "walletType", 1, "guid")).toDF("customer_id", "wallet_id", "wallet_type", "trust_factor", "guid")
      val walletDetails = Seq((1, 20.0, 2, "guid")).toDF("wallet_id", "balance", "id", "owner_guid")
      val ppiDetails = Seq((10, 21.0, 2, 123, 2, Date.valueOf("2018-11-06"), Timestamp.valueOf("2018-11-06 12:25:09")), (10, 101.0, 2, 123, 2, Date.valueOf("2018-11-06"), Timestamp.valueOf("2018-11-06 12:25:10")), (10, 100.0, 2, 123, 2, Date.valueOf("2018-11-06"), Timestamp.valueOf("2018-11-06 12:25:08"))).toDF("id", "balance", "ppi_type", "merchant_id", "parent_wallet_id", "dt", "create_timestamp")

      walletUser.printSchema()
      walletDetails.printSchema()
      ppiDetails.printSchema()

      val walluser = walletUser.select($"customer_id", $"guid")
      val walldetail = walletDetails.select($"id", $"owner_guid")
      val ppidetails = ppiDetails.select($"id".as("sub_wallet_id"), $"balance", $"merchant_id", $"parent_wallet_id", $"dt", $"ppi_type", $"create_timestamp").withColumn("PPI_type", PPItypeByppitype)

      val subwallintermediate = walluser
        .join(walldetail, $"guid" === $"owner_guid")
        .join(ppidetails, $"id" === $"parent_wallet_id")
        .select($"customer_id", $"dt", $"PPI_type".as("ppi_type"), $"sub_wallet_id", $"merchant_id".as("issuer_id"), $"balance", $"create_timestamp")

      subwallintermediate.show()

      val df2 = subwallintermediate
        .as[test1]
        .groupByKey(key => (key.customer_id, key.dt, key.ppi_type, key.issuer_id))
        .reduceGroups((r1, r2) => if (r1.create_timestamp.after(r2.create_timestamp)) r1 else r2)
        .map(_._2)
        .toDF(
          "customer_id",
          "dt",
          "ppi_type",
          "sub_wallet_id",
          "issuer_id",
          "balance",
          "create_timestamp"
        ).show()

      assert(true)
    }
  }
}