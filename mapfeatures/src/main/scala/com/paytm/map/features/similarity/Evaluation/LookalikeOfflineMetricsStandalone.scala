package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{avg, explode, udf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object LookalikeOfflineMetricsStandalone extends LookalikeOfflineMetricsStandaloneJob with SparkJob with SparkJobBootstrap {

}

trait LookalikeOfflineMetricsStandaloneJob {
  this: SparkJob =>

  val JobName = "LookalikeOfflineMetricsStandaloneJob"

  private val customerGroupSeq = 1 to 3
  private val flatTableBasePath = "s3a://midgar-aws-workspace/prod/mapfeatures/features/"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val seedPath = args(1)
    val boostOldPath = args(2)
    val boostNewPath = args(3)
    val segmentId = args(4)

    val targetDate = ArgsUtils.getTargetDate(args).minusDays(1)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val exeTime = ArgsUtils.getTargetDate(args)
    val exeTimeStr = exeTime.toString(ArgsUtils.formatter)

    // metrics outputs path
    val evalOutputPath = s"${settings.featuresDfs.lookAlike}/metrics/$segmentId/$exeTimeStr"
    val metricsPath = s"$evalOutputPath/${segmentId}_metrics"

    // loading boosting audience
    val boostSeg = loadSegment(spark, boostOldPath, 0)
    val boostSize = boostSeg.count().toDouble;

    val boostSegN = loadSegment(spark, boostNewPath, 2)
    val boostSizeN = boostSegN.count().toDouble;

    val boostFuzzy = loadSegment(spark, boostNewPath, 1)
    val boostFuzzySize = boostFuzzy.count().toDouble;

    // loading seed audience
    val seedSeg = loadSeed(spark, seedPath)
    val seedSize = seedSeg.count().toDouble

    val baseData = loadBase(spark, targetDateStr)

    /// metric 1
    // seed
    val loginEventSeed = loginEvent(spark, baseData, targetDate, targetDateStr, seedSeg, seedSize)
    val loginEventExact = loginEvent(spark, baseData, targetDate, targetDateStr, boostSeg, boostSize)
    val loginEventExactFuzzy = loginEvent(spark, baseData, targetDate, targetDateStr, boostSegN, boostSizeN)
    val loginEventFuzzy = loginEvent(spark, baseData, targetDate, targetDateStr, boostFuzzy, boostFuzzySize)

    /// metric 2
    val avgTraxCountLast7DaysSeed = avgTraxCountLast7Days(spark, baseData, seedSeg)
    val avgTraxCountLast7DaysExact = avgTraxCountLast7Days(spark, baseData, boostSeg)
    val avgTraxCountLast7DaysExactFuzzy = avgTraxCountLast7Days(spark, baseData, boostSegN)
    val avgTraxCountLast7DaysFuzzy = avgTraxCountLast7Days(spark, baseData, boostFuzzy)

    /// metric 3
    // seed
    val promocodeUsageSeed = promocodeUsage(spark, baseData, targetDate, targetDateStr, seedSeg, seedSize)
    val promocodeUsageExact = promocodeUsage(spark, baseData, targetDate, targetDateStr, boostSeg, boostSize) // exact
    val promocodeUsageExactFuzzy = promocodeUsage(spark, baseData, targetDate, targetDateStr, boostSegN, boostSizeN) // fussy+exact
    val promocodeUsageFuzzy = promocodeUsage(spark, baseData, targetDate, targetDateStr, boostFuzzy, boostFuzzySize) // fussy

    val results = Map("seed_Login_last_30_days_ratio" -> loginEventSeed, "old_boost_Login_last_30_days_ratio" -> loginEventExact,
      "new_boost_Login_last_30_days_ratio" -> loginEventExactFuzzy, "fuzzy_boost_Login_last_30_days_ratio" -> loginEventFuzzy,
      "seed_avg_tranx_count_last_7_days" -> avgTraxCountLast7DaysSeed, "old_boost_avg_tranx_count_last_7_days" -> avgTraxCountLast7DaysExact,
      "new_boost_avg_tranx_count_last_7_days" -> avgTraxCountLast7DaysExactFuzzy, "fuzzy_boost_avg_tranx_count_last_7_days" -> avgTraxCountLast7DaysFuzzy,
      "seed_promocode_usage_ratio_in_total" -> promocodeUsageSeed._1, "seed_promocode_usage_ratio_last_30_days" -> promocodeUsageSeed._2, "seed_avg_promocode_usage_count_last_30_days" -> promocodeUsageSeed._3,
      "old_boost_promocode_usage_ratio_in_total" -> promocodeUsageExact._1, "old_boost_promocode_usage_ratio_last_30_days" -> promocodeUsageExact._2, "old_boost_avg_promocode_usage_count_last_30_days" -> promocodeUsageExact._3,
      "new_boost_promocode_usage_ratio_in_total" -> promocodeUsageExactFuzzy._1, "new_boost_promocode_usage_ratio_last_30_days" -> promocodeUsageExactFuzzy._2, "new_boost_avg_promocode_usage_count_last_30_days" -> promocodeUsageExactFuzzy._3,
      "fuzzy_boost_promocode_usage_ratio_in_total" -> promocodeUsageFuzzy._1, "fuzzy_boost_promocode_usage_ratio_last_30_days" -> promocodeUsageFuzzy._2, "fuzzy_boost_avg_promocode_usage_count_last_30_days" -> promocodeUsageFuzzy._3)

    /// save metrics
    saveMetrics(spark, results, metricsPath)
  }

  def saveMetrics(
    spark: SparkSession,
    metrics: Map[String, Double],
    metricsPath: String
  ): Unit = {
    import spark.implicits._
    val df = metrics.toSeq.toDF("name", "score")
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(metricsPath)
  }

  def getSamplingGroup(spark: SparkSession, exeTime: DateTime, evalOutputPath: String, sampleCount: Int): DataFrame = {
    val exeTimeStr = exeTime.minusDays(1).toString(ArgsUtils.formatter)
    val baseData = customerGroupSeq.map(x => {
      val flatPathStr = s"${flatTableBasePath}$x/flatTable/dt=$exeTimeStr"
      val df = spark.read.load(flatPathStr).select("customer_id")
      df //return
    }).reduce(_ union _)
    val sample20m = baseData.sample(false, 1D * sampleCount / baseData.count)
    sample20m.write.mode(SaveMode.Overwrite).parquet(s"$evalOutputPath/sampled_customers_$exeTimeStr")
    sample20m
  }

  def loadBase(spark: SparkSession, targetDateStr: String): DataFrame = {
    customerGroupSeq.map(x => {
      spark.read.load(s"$flatTableBasePath$x/flatTable/dt=$targetDateStr").select("customer_id", "PAYTM_promo_usage_dates", "PAYTM_total_transaction_count_7_days", "latest_open_date")
    }).reduce(_ union _)
  }

  def promocodeUsage(
    spark: SparkSession,
    baseData: DataFrame,
    targetDate: DateTime,
    targetDateStr: String,
    data: DataFrame,
    size: Double
  ): (Double, Double, Double) = {

    import spark.implicits._
    val promousage = baseData.select($"customer_id", explode($"PAYTM_promo_usage_dates.Date") as "promo").filter($"promo".isNotNull)

    val endDate = targetDate.minusDays(30)
    val endDataStr = endDate.toString(ArgsUtils.formatter)

    // seed
    val avg_promo_30 = promousage.filter($"promo" > endDataStr).join(data, $"customer_id" === $"cid")
    val promo = promousage.dropDuplicates("customer_id").join(data, $"customer_id" === $"cid")
    val promo_30 = avg_promo_30.dropDuplicates("customer_id")

    val promocode_usage_ratio_in_total = promo.count().toDouble / size
    val promocode_usage_ratio_last_30_days = promo_30.count().toDouble / size
    val avg_promocode_usage_count_last_30_days = avg_promo_30.count().toDouble / size

    (promocode_usage_ratio_in_total, promocode_usage_ratio_last_30_days, avg_promocode_usage_count_last_30_days)
  }

  def avgTraxCountLast7Days(
    spark: SparkSession,
    baseData: DataFrame,
    data: DataFrame
  ): Double = {

    import spark.implicits._

    val tranxcount7days = baseData.select($"customer_id", $"PAYTM_total_transaction_count_7_days" as ("total")).filter($"total".isNotNull)
    val trans_ov1_7 = tranxcount7days.join(data, $"customer_id" === $"cid").agg(avg("total"))
    trans_ov1_7.head().getDouble(0)
  }

  def loginEvent(
    spark: SparkSession,
    baseData: DataFrame,
    targetDate: DateTime,
    targetDateStr: String,
    data: DataFrame,
    size: Double
  ): (Double) = {

    import spark.implicits._
    val endDate = targetDate.minusDays(30)
    val endDataStr = endDate.toString(ArgsUtils.formatter)

    val latestOpen = baseData.select("customer_id", "latest_open_date").where($"latest_open_date".isNotNull && $"latest_open_date" > endDataStr)
    val ov1 = data.join(latestOpen, $"customer_id" === $"cid")

    val login_last_30_days_ratio = ov1.count() / size
    login_last_30_days_ratio
  }

  def loadCsvSegment(
    spark: SparkSession,
    path: String
  ): DataFrame = {

    import spark.implicits._

    val isNumeric = udf { customerId: String => customerId.forall(c => c.isDigit) }

    spark.read.option("header", "false").csv(path)
      .withColumnRenamed("_c0", "customer_id")
      .filter($"customer_id".isNotNull && isNumeric($"customer_id"))
      .select($"customer_id".cast(LongType) as "cid")
  }

  def loadSegment(
    spark: SparkSession,
    path: String,
    old: Int
  ): DataFrame = {
    import spark.implicits._
    old match {
      case 0 => spark.read.load(path).filter($"max_kendalltau".equalTo(1.0)).select($"customer_id" cast (LongType) as "cid").distinct()
      case 1 => spark.read.load(path).filter($"max_kendalltau" < 1.0).select($"customer_id" cast (LongType) as "cid").distinct()
      case _ => spark.read.load(path).select($"customer_id" cast (LongType) as "cid").distinct()
    }
  }

  def loadSeed(
    spark: SparkSession,
    path: String
  ): DataFrame = {
    import spark.implicits._
    spark.read.load(path).select($"customer_id" cast (LongType) as "cid").distinct()
  }
}

