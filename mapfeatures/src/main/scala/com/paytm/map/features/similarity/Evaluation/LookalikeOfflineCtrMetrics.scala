package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object LookalikeOfflineCtrMetrics extends LookalikeOfflineCtrMetricsJob with SparkJob with SparkJobBootstrap

trait LookalikeOfflineCtrMetricsJob {
  this: SparkJob =>

  val JobName = "LookalikeOfflineCtrMetricsJob"

  private val customerGroupSeq = 1 to 3
  private val flatTableBasePath = "s3a://midgar-aws-workspace/prod/mapfeatures/features/"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    import spark.implicits._

    val seedPath = args(1)
    val boostOldPath = args(2)
    val boostNewPath = args(3)

    val targetDate = DateTime.parse(args(4))
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val minimum_customer_count = args(5).toInt

    val exeTime = ArgsUtils.getTargetDate(args)
    val exeTimeStr = exeTime.toString(ArgsUtils.formatter)

    val evalOutputPath = s"${settings.featuresDfs.lookAlike}/metrics/ctr/$targetDateStr/$exeTimeStr/"
    val dateStrSeq = generateDateInterval(targetDate)

    val seedSeg = loadSeed(spark, seedPath)

    val boostExact = boostNewPath.contains(".csv") match {
      case true => loadCsvSegment(spark, boostOldPath)
      case _    => loadSegment(spark, boostNewPath, 0)
    }

    val boostFuzzy = (boostNewPath.contains(".csv")) match {
      case true => loadCsvSegment(spark, boostNewPath)
      case _    => loadSegment(spark, boostNewPath, 1)
    }

    val boostExactFuzzy = (boostNewPath.contains(".csv")) match {
      case true => loadCsvSegment(spark, boostNewPath)
      case _    => loadSegment(spark, boostNewPath, 2)
    }

    val bannerUniverse = loadUniverse(spark, dateStrSeq)
    val seedCtr = bannerCTR(spark, bannerUniverse, seedSeg).select($"slot", $"CTR" as "CTR_seed", $"customer_id_count" as "customer_id_count_seed")
    val ctrExact = bannerCTR(spark, bannerUniverse, boostExact).select($"slot", $"CTR" as "CTR_exact", $"customer_id_count" as "customer_id_count_exact")
    val ctrFuzzy = bannerCTR(spark, bannerUniverse, boostFuzzy).select($"slot", $"CTR" as "CTR_fuzzy", $"customer_id_count" as "customer_id_count_fuzzy")
    val ctrExactFuzzy = bannerCTR(spark, bannerUniverse, boostExactFuzzy).select($"slot", $"CTR" as "CTR_exact_fuzzy", $"customer_id_count" as "customer_id_count_exact_fuzzy")

    val metrics = ctrByCommonSlots(spark, seedCtr, ctrExact, ctrFuzzy, ctrExactFuzzy, minimum_customer_count)

    saveMetrics(spark, seedCtr, s"$evalOutputPath/seed")
    saveMetrics(spark, ctrExact, s"$evalOutputPath/exact")
    saveMetrics(spark, ctrFuzzy, s"$evalOutputPath/fuzzy")
    saveMetrics(spark, ctrExactFuzzy, s"$evalOutputPath/exactFuzzy")
    saveMetrics(spark, metrics, s"$evalOutputPath/metrics")
  }

  def ctrByCommonSlots(
    spark: SparkSession,
    seedCtr: DataFrame,
    ctrExact: DataFrame,
    ctrFuzzy: DataFrame,
    ctrExactFuzzy: DataFrame,
    minimum_customer_count: Int
  ): DataFrame = {

    import spark.implicits._

    val metrics = seedCtr.join(ctrExact, Seq("slot"), "inner").join(ctrFuzzy, Seq("slot"), "inner").join(ctrExactFuzzy, Seq("slot"), "inner").distinct()
      .filter($"customer_id_count_seed" > minimum_customer_count && $"customer_id_count_exact" > minimum_customer_count && $"customer_id_count_exact_fuzzy" > minimum_customer_count && $"customer_id_count_fuzzy" > minimum_customer_count)
      .agg(mean("CTR_seed").as("avg_ctr_seed"), mean("CTR_exact").as("avg_ctr_exact"), mean("CTR_exact_fuzzy").as("avg_ctr_exact_fuzzy"), mean("CTR_fuzzy").as("avg_ctr_fuzzy"))

    metrics
  }

  def saveMetrics(
    spark: SparkSession,
    metrics: DataFrame,
    metricsPath: String
  ): Unit = {

    metrics.write
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

  def generateDateInterval(targetDate: DateTime): Seq[String] = {
    val days = 0 to 2
    val intervals = days.map(x => targetDate.minusDays(x).toString(ArgsUtils.formatter))
    intervals
  }

  def loadCsvSegment(spark: SparkSession, userBasePath: String): DataFrame = {

    import spark.implicits._

    val isNumeric = udf { customerId: String => customerId.forall(c => c.isDigit) }

    spark.read.option("header", "false").csv(userBasePath)
      .withColumnRenamed("_c0", "customer_id")
      .filter($"customer_id".isNotNull && isNumeric($"customer_id"))
      .select($"customer_id" cast (LongType) as "customer_id")
  }

  def loadSegment(
    spark: SparkSession,
    path: String,
    audienceFilter: Int
  ): DataFrame = {
    import spark.implicits._
    audienceFilter match {
      case 0 => spark.read.load(path).filter($"max_kendalltau".equalTo(1.0)).select($"customer_id" cast (LongType) as "customer_id").distinct()
      case 1 => spark.read.load(path).filter($"max_kendalltau" < 1.0).select($"customer_id" cast (LongType) as "customer_id").distinct()
      case _ => spark.read.load(path).select($"customer_id" cast (LongType) as "customer_id").distinct()
    }
  }

  def loadUniverse(
    spark: SparkSession,
    dateStrSeq: Seq[String]
  ): DataFrame = {

    import spark.implicits._

    val bannerUniverse = dateStrSeq.map(x => {
      val dateStr = x
      val bannerPath = s"s3a://midgar-aws-workspace/prod/mapmeasurements/banner/signal_internal_promotion/dt=$dateStr"
      spark.read.load(bannerPath).select("customer_id", "date", "click", "slot", "banner_id", "impression", "channel", "carousel_name")
        .where(!$"carousel_name".contains("icon")) //no icon
        .where($"channel".equalTo("app"))

    }).reduce(_ union _)

    bannerUniverse
  }

  def bannerCTR(
    spark: SparkSession,
    bannerUniverse: DataFrame,
    data: DataFrame
  ): DataFrame = {

    import spark.implicits._

    val ctr = data.join(bannerUniverse, Seq("customer_id"))
      .groupBy("slot")
      .agg(sum("click").as("clickSum"), sum("impression").as("impressionSum"), count("customer_id").as("customer_id_count"))
      .withColumn("CTR", $"clickSum" / $"impressionSum").select("slot", "CTR", "customer_id_count")
    ctr
  }

  def loadSeed(
    spark: SparkSession,
    path: String
  ): DataFrame = {
    import spark.implicits._
    spark.read.load(path).select($"customer_id" cast (LongType) as "customer_id").distinct()
  }
}
