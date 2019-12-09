package com.paytm.map.features.utils

import com.codahale.metrics.Gauge
import com.paytm.map.features.base.BaseTableUtils.{AppCategoryCnt, OperatorCnt}
import com.paytm.map.features.datasets.PayAliMappingRecord
import com.paytm.map.features.utils.ArgsUtils.formatter
import com.paytm.map.features.utils.monitoring.FeaturesMonitor
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.coursera.metrics.datadog.TaggedName.TaggedNameBuilder
import org.joda.time.Days

//TODO: The functions
object UDFs {
  val logger = Logger.getLogger(getClass)

  val isAllNumeric: UserDefinedFunction = udf { customerId: String =>
    customerId.forall(c => c.isDigit)
  }

  val standardizeGender: UserDefinedFunction = udf[String, String] {
    genderString =>
      genderString.toLowerCase match {
        case "male"   => "Male"
        case "female" => "Female"
        case "1"      => "Male"
        case "2"      => "Female"
        case _        => null
      }
  }

  /** Reads a seq of provided parquet paths and returns seq of dataframe * */
  def readPathsAll(spark: SparkSession, paths: Seq[String], broadcastPaths: Seq[String] = Seq()): Seq[DataFrame] = {
    import org.apache.spark.sql.functions.broadcast

    for (path <- paths) yield {
      if (broadcastPaths.contains(path)) broadcast(spark.read.parquet(path))
      else spark.read.parquet(path)
    }
  }

  //functions to read data
  def readTableV3(spark: SparkSession, path: Map[String, String]): DataFrame = {
    val pathList = path.values.map(partnPath => partnPath.replace("s3:", "s3a:")).toSeq
    spark.read.parquet(pathList: _*)
  }

  def readTableV3(spark: SparkSession, partitionKeyToS3Paths: Map[String, String], maxDate: String, minDate: String = "1970-01-01"): DataFrame = {
    val pathList = partitionKeyToS3Paths
      .filter(r => r._1.split("=").last <= maxDate && r._1.split("=").last >= minDate)
      .values
      .map(partnPath => partnPath.replace("s3:", "s3a:"))
      .toSeq

    // if there are stale data, send number of missing days to datadog
    val lastAvailableDate = partitionKeyToS3Paths.keys.toSeq.max.split("=").last
    if (lastAvailableDate < maxDate) {
      val metricName = "daas_data_missing_days"
      val metricValue = Days.daysBetween(formatter.parseDateTime(lastAvailableDate), formatter.parseDateTime(maxDate)).getDays
      val datasetName = partitionKeyToS3Paths.values.head.split('/').slice(4, 6).mkString(".")
      gaugeValuesWithTags(
        metricName,
        Map("dataset" -> datasetName),
        metricValue
      )
    }

    if (pathList.nonEmpty)
      spark.read.parquet(pathList: _*)
    else {
      logger.error(s"No partition found between minDate: $minDate, maxDate: $maxDate. " +
        s"Dataset ${partitionKeyToS3Paths.head._2} only available between ${partitionKeyToS3Paths.keySet.toSeq.min} and ${partitionKeyToS3Paths.keySet.toSeq.max}. Continue with empty dataset")

      val existingPath = partitionKeyToS3Paths.values.head.replace("s3:", "s3a:")
      val schema = spark.read.parquet(existingPath).schema
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
  }

  def readTableV3Merge(spark: SparkSession, path: Map[String, String]): DataFrame = {
    val pathList = path.values.map(partnPath => partnPath.replace("s3:", "s3a:")).toSeq
    spark.read.option("mergeSchema", "true").parquet(pathList: _*)
  }

  def readTableV3Since(spark: SparkSession, path: Map[String, String], minDate: String): DataFrame = {
    readTableV3(spark, path.filter(r => r._1.split("=").last >= minDate))
  }

  def getPaytmAPlusMapping(
    spark: SparkSession,
    mappingPaths: Map[String, String],
    targetDateStr: String
  ): DataFrame = {
    import spark.implicits._

    val w = Window.partitionBy("paytm_user_id").orderBy($"update_time".desc)
    readTableV3(spark, mappingPaths, targetDateStr)
      .filter($"name" === "MIGRATE_IP_UK")
      .filter($"value".isNotNull)
      .select($"value" as "paytm_user_id", $"principal_id" as "alipay_user_id", $"gmt_modified" as "update_time")
      .as[PayAliMappingRecord]
      .select(
        first($"paytm_user_id").over(w) as "customer_id",
        first($"alipay_user_id").over(w) as "alipay_user_id"
      )
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def readTableV2(spark: SparkSession, dbName: String, tableName: String, maxDate: String, minDate: String = "1970-01-01", partnCol: String = "dl_last_updated"): DataFrame = {

    println(s"Reading the Table $dbName.$tableName")
    import spark.implicits._
    val tablePath = {
      import org.apache.spark.sql.Row
      //getLatestSnapshot
      val bucketName: String = s"daas-datalake-midgar-dropoff"
      val snapshot = spark.read.json(s"s3a://$bucketName/.meta/$dbName.json").map { case Row(a: String) => a }.collect().head
      dbName match {
        case "dwh" => s"s3a://$bucketName/$dbName/hive/.snapshot/$snapshot/$dbName.db/$tableName/"
        case _     => s"s3a://$bucketName/apps/hive/warehouse/$dbName.db/.snapshot/$snapshot/$tableName/"
      }
    }

    spark.read.parquet(tablePath)
      .where(col(partnCol) <= maxDate)
      .where(col(partnCol) >= minDate)
      .drop(partnCol)
  }

  def readTable(spark: SparkSession, pathList: Map[String, String], dbName: String, tableName: String, maxDate: String, minDate: String = "1970-01-01", isV2: Boolean = false, partnCol: String = "dl_last_updated"): DataFrame = {
    if (isV2) readTableV2(spark, dbName, tableName, maxDate, minDate, partnCol) else readTableV3(spark, pathList, maxDate, minDate)
  }

  def gaugeValuesWithTag(
    metricName: String,
    tagName: String,
    tagValue: String,
    metricValue: Any
  ): Gauge[Any] = {

    val metName = new TaggedNameBuilder().metricName(metricName)
    val metricNameWithTag: String = metName
      .addTag(s"$tagName:$tagValue")
      .build()
      .encode()

    gaugeValues(metricNameWithTag, metricValue)
  }

  def gaugeValuesWithTags(metricName: String, tagMap: Map[String, String],
    metricValue: Any): Gauge[Any] = {
    val tagBuilder = new TaggedNameBuilder()

    tagMap.foreach {
      case (tagName, tagValue) =>
        tagBuilder.addTag(s"$tagName:$tagValue")
    }
    val metricNameWithTag = tagBuilder
      .metricName(metricName)
      .build()
      .encode()

    gaugeValues(metricNameWithTag, metricValue)
  }

  def gaugeValues(metricName: String, metricValue: Any): Gauge[Any] = {
    println(metricName, metricValue)
    val monitor = FeaturesMonitor.getJobMonitor("")
    monitor.gauge(metricName, () => metricValue)
  }

  def getMinMax(df: DataFrame, colName: String): (Double, Double) = {
    val minMax = df.select(
      min(colName) as s"min_$colName",
      max(colName) as s"max_$colName"
    ).head
    (minMax.getAs[Double](s"min_$colName"), minMax.getAs[Double](s"max_$colName"))
  }

  val strListToOperatorSeq = (optList: Seq[String]) => {
    optList.filter(_ != null)
      .groupBy(identity).mapValues(_.size)
  }

  val toOperatorCnt: UserDefinedFunction = udf[Seq[OperatorCnt], Seq[String]](
    strListToOperatorSeq.andThen(maps => maps.map(x => OperatorCnt(x._1, x._2)).toSeq)
  )

  val toAppCategoryCount: UserDefinedFunction = udf[(Option[String], Seq[AppCategoryCnt]), Seq[String]] {
    strListToOperatorSeq.andThen(maps => {
      val max = if (maps.isEmpty) None else Some(maps.maxBy(_._2)._1)
      (max, maps.map(x => AppCategoryCnt(x._1, x._2)).toSeq)
    })
  }

  val flattenSeqOfSeq = udf((SeqofSeqCol: Seq[Seq[String]]) => SeqofSeqCol.flatten)

  /** After using toOperatorCnt, get operator with highest count. Same as getMaxOperator except row is already flattened */
  import com.paytm.map.features.base.BaseTableUtils.BinaryOperatorCnt

  import scala.reflect.runtime.universe.TypeTag
  def getPrefOperator[A <: Product: TypeTag](implicit ev: BinaryOperatorCnt[A], nestedSchema: Array[StructField]): UserDefinedFunction = udf[Option[String], Seq[Row]] { x =>
    val allOpGrouped = x.map {
      x => (x.getAs[String](nestedSchema(0).name), x.getAs[Long](nestedSchema(1).name))
    }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
    if (allOpGrouped.isEmpty) null
    else Some(allOpGrouped.maxBy(_._2)._1)
  }

  val collectUnique: UserDefinedFunction = udf[Seq[String], Seq[Seq[String]]]((seqOfSeqX: Seq[Seq[String]]) => seqOfSeqX.flatten.distinct)

  val withPrefixes: String => Seq[String] = { str =>
    if (str == null) Seq()
    else (1 to str.length).map(end => str.substring(0, end))
  }
}

object Percent {
  val One = Percent(1.0)
  val Zero = Percent(0.0)

  def apply(amount: Double): Percent = {
    require(amount >= 0 && amount <= 100, s"Percentages passed should be between 0 and 100. Passed $amount")
    new Percent(amount)
  }
}

class Percent(val amount: Double) {
  def getAsDouble: Double = amount / 100.0
}

