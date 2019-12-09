package com.paytm.map.features.channel

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.{BaseTableUtils, DataTables}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.functions.{date_add, _}
import org.apache.spark.sql._
import org.joda.time.DateTime
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object ChannelFeatures extends ChannelFeaturesJob
  with SparkJob with SparkJobBootstrap

trait ChannelFeaturesJob {
  this: SparkJob =>

  val JobName = "ChannelFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val startDate: DateTime = targetDate.minusDays(lookBackDays)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = startDate.toString(ArgsUtils.formatter)
    val dtSeq = BaseTableUtils.dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Output path
    val outPath = s"${settings.featuresDfs.baseDFS.aggPath}/ChannelFeatures/$targetDateStr"

    val channelFeaturesDf: DataFrame = channelFeatures(settings, startDateStr, targetDateStr).persist(StorageLevel.MEMORY_AND_DISK)

    channelFeaturesDf
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outPath)

    getCountDf(spark.read.parquet(outPath))
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"${settings.featuresDfs.baseDFS.aggPath}/ChannelFeaturesCustomerCount/$targetDateStr")
  }

  def channelFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val baseTransactions = getBaseTransactions(settings, targetDateStr)
    val merchantTransactions = getBaseTransactionsWithMerchant(settings, targetDateStr)
    val profile = getProfile(settings, targetDateStr)
    val selling_price_30 = "selling_price_within_30"

    val filteredColsDf =
      merchantTransactions
        .columnLookbackFilter(selling_price_30, "txn_amount", "created_at", 30, targetDateStr)

    val merchantFeatures = //merchants determined by shop_ids. mapping provided by channels team
      filteredColsDf
        .where($"created_at" > date_add(lit(targetDateStr), -90))
        .groupBy("customer_id", "shop_id")
        .agg(
          to_date(max(to_date(col("created_at")))) as "last_transaction_date",
          sum(coalesce(col(selling_price_30), lit(0))) as "total_transactions_month_amount"
        )
        .withColumn("merchant", struct($"shop_id", $"last_transaction_date", $"total_transactions_month_amount"))
        .groupBy("customer_id")
        .agg(collect_list("merchant") as "merchants")

    val avgMonthlyTransactionAmount = baseTransactions
      .where($"created_at" > date_add(lit(targetDateStr), -180))
      .groupBy(month($"created_at") as "created_month", $"customer_id")
      .agg(sum($"txn_amount") as "selling_price_month")
      .groupBy("customer_id")
      .agg(avg($"selling_price_month") as "average_monthly_transaction_amount")

    avgMonthlyTransactionAmount
      .join(merchantFeatures, Seq("customer_id"), "left")
      .join(profile, Seq("customer_id"), "left")
  }

  def getBaseTransactionsWithMerchant(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    val newStr = getBaseTransactions(settings, targetDateStr)
    val merchant = spark.read.parquet(settings.featuresDfs.baseDFS.merchantsPath).select("mid", "merchant_id").distinct()
    val shopIdMapping = spark.read.parquet(s"${settings.featuresDfs.baseDFS.aggPath}/ChannelFeaturesShopIdMapping")

    val merchantShopId = merchant.join(shopIdMapping, merchant("mid") === shopIdMapping("pg_mid"), "inner")
    val merchantPath = s"${settings.featuresDfs.baseDFS.aggPath}/ChannelMerchantJoined/"
    merchantShopId.write.mode(SaveMode.Overwrite).parquet(merchantPath)
    spark.read.parquet(merchantPath)

    val base =
      newStr
        .join(merchantShopId, Seq("merchant_id"), "inner")
        .select(
          merchantShopId("shop_id"),
          newStr("dt"),
          newStr("txn_amount"),
          newStr("customer_id"),
          newStr("created_at") as "created_at"
        )
    val tmpPath = s"${settings.featuresDfs.baseDFS.aggPath}/ChannelBaseTransactions/"
    base.write.mode(SaveMode.Overwrite).parquet(tmpPath)
    spark.read.parquet(tmpPath)
  }

  def getBaseTransactions(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    spark.read.parquet(settings.featuresDfs.baseDFS.strPath)
      .where($"txn_status" === 1)
      .where(!$"txn_type".isin(5, 20))
      .where($"created_at" > date_add(lit(targetDateStr), -180))
      .withColumn("dt", to_date($"dt"))
      .withColumn("txn_amount", $"txn_amount".cast("Double"))
      .withColumn("merchant_id", $"payee_id")
  }

  def getProfile(settings: Settings, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    val profilePath = s"${settings.featuresDfs.featuresTable}/${settings.featuresDfs.profile}dt=$targetDateStr"
    val clean = udf(cleanPhone)
    spark.read.parquet(profilePath)
      .select(col("customer_id"), clean(col("phone_no")).as("phone_no"))
  }

  def cleanPhone: String => String = s => {
    if (s == null) null
    else s.filter(_.isDigit)
  }

  def getCountDf(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val someData = Seq(Row(df.count()))
    val countSchema = List(
      StructField("count", LongType, false)
    )
    spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(countSchema)
    )
  }

}
