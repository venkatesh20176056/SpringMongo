package com.paytm.map.features.measurements

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm
import com.paytm.map.features.utils.FileUtils.{getAvlTableDates, getLatestTableDate}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import scala.io.Source.fromInputStream
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import java.nio.charset.CodingErrorAction

import com.paytm.map.features.utils.ArgsUtils.formatter
import org.apache.commons.math3.exception.util.ArgUtils

import scala.io.Codec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row

// Defines the json structure in Inclusion and Exclusion Rules
case class Operand(dataType: String, op: String, operands: Array[String])
case class AndOperrand(op: String, operands: Array[Operand], type1: String)
case class OrOperand(op: String, operands: Array[AndOperrand], type1: String)
case class puClass(id: Long, createdAt: String, firstOccurrence: String, nextOccurrence: String, Rules: Array[OrOperand])

object GeneratePropertyUsageReport extends GeneratePropertyUsageReportJob
  with SparkJob with SparkJobBootstrap

trait GeneratePropertyUsageReportJob {
  this: SparkJob =>

  val JobName = "GeneratePropertyUsageReport"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    // Get the command line parameters
    val dt: DateTime = ArgsUtils.getTargetDate(args).plusDays(1)
    val lookBackDays: Int = args(1).toInt

    val targetDate: String = dt.toString(ArgsUtils.formatter)
    // sequence of dates for which data is required
    val daySeq: Seq[String] = dayIterator(dt.minusDays(lookBackDays), dt, ArgsUtils.formatter)

    // Paths
    val groupPrefix = "1"
    //val featFlatTablePath = s"${settings.featuresDfs.featuresTable}/$groupPrefix/${settings.featuresDfs.joined}"
    // raw day property usages will be written here
    val propertyUsageRawPath = s"${settings.featuresDfs.measurementPath}" + "raw/"
    // reports will be here
    val propertyUsageReportPath = s"${settings.featuresDfs.measurementPath}" + "report/"
    // url to get campaign descriptions
    val baseURLCampaigns = "http://internal-map-cma-production-lb-1488664899.ap-south-1.elb.amazonaws.com/clients/paytm-india/management/executions"

    // reads json content for url http://baseURL/dateReq
    def readCampaigns(baseURL: String, dateReq: String): String =
      {
        implicit val formats = DefaultFormats
        val httpClient = HttpClientBuilder.create.build
        val httpResponse = httpClient.execute(new HttpGet(s"$baseURL/$dateReq"))
        val responseCode = httpResponse.getStatusLine.getStatusCode

        implicit val codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

        val inputStream = httpResponse.getEntity.getContent
        val content = fromInputStream(inputStream).getLines.mkString

        inputStream.close()
        httpClient.close()

        content
      }

    // convert json to corresponding dataframe (schema inferred automatially)
    def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
      val reader = spark.read
      Option(schema).foreach(reader.schema)
      reader.json(spark.sparkContext.parallelize(Array(json)))
    }

    def resolveRules(x: OrOperand): Seq[String] = {
      x.asInstanceOf[OrOperand].operands.flatMap(_.operands.map(_.operands(0)))
    }

    // extracting rules from the nested strcutre for rules
    // as described in case class puClass (aka property usage class)
    def extractRules(x: puClass) = (
      x.id,
      x.createdAt.split("T")(0),
      x.firstOccurrence.split("T")(0),
      x.nextOccurrence.split("T")(0),
      x.Rules.flatMap(resolveRules)
    )

    // Finding out for which dates data is not present
    val alreadyPresentDates = getAvlTableDates(propertyUsageRawPath, spark, "dt=")
    val requiredDates = daySeq.diff(alreadyPresentDates)

    println("TargetDate: " + targetDate)
    println("AlreadyPresentDates: " + alreadyPresentDates.length.toString)
    println("DaySeq: " + daySeq.length.toString)
    println("REquired Run for Dates: " + requiredDates.length.toString)

    // for each date for which we dont have raw usage data
    requiredDates.foreach(reqDt => {

      // read json content
      val content = readCampaigns(baseURLCampaigns, reqDt)

      // converting json to df
      // after replacing `type` keys to `type1` (conflicts with scala reserved kwd type in case class)
      val jsonDf = jsonToDataFrame(content.replace("\"type\":", "\"type1\":"))

      println(reqDt, jsonDf.count())

      if (jsonDf.count() >= 1) {

        // Treating exclusion and inclusion rules separately, will join them later
        val jsonDfIncl = jsonDf.select($"id", $"createdAt", $"scheduling.firstOccurrence", $"nextOccurrence", $"inclusion.rules" as "Rules")
        val jsonDfExcl = jsonDf.select($"id", $"createdAt", $"scheduling.firstOccurrence", $"nextOccurrence", $"exclusion.rules" as "Rules")

        val extractedData = Seq(jsonDfIncl, jsonDfExcl).map(
          jsonData => {
            try {
              // coercing to propertyUsage class (puClass)
              val jsonDataParsed = jsonData.as[puClass]

              // for each row, apply extractRules from which rules are extracted
              // rules correspond to inclusion and exclusion rules respectively
              // and consists of Seq of Operands (properties)
              val extractedRDD = spark.sparkContext.parallelize(jsonDataParsed.collect().map(extractRules(_)))
              extractedRDD.toDF("id", "created_at", "first_usage_at", "next_usage_at", "rules")
            } catch {
              case e: Throwable =>
                println(s"Exception: Could not cast either InclRules or ExclRules column to puClass" +
                  "\n This happens when there are 0 rules (either Incl or Excl) for given date: " + reqDt)

                val extractedRDDSchema = StructType(
                  StructField("id", LongType, nullable = true) ::
                    StructField("created_at", StringType, nullable = true) ::
                    StructField("first_usage_at", StringType, nullable = true) ::
                    StructField("next_usage_at", StringType, nullable = true) ::
                    StructField("rules", ArrayType(StringType, containsNull = true), nullable = true) :: Nil
                )

                // Empty dataframe in case of exception, to support union
                spark.createDataFrame(spark.sparkContext.emptyRDD[Row], extractedRDDSchema)
            }
          }

        ).reduce(_ union _)

        // since both inclusion and exclusion properties are required
        // simply union them for aggregate later
        val propertyUsageData = extractedData.select(
          $"id",
          $"created_at",
          $"first_usage_at",
          $"next_usage_at",
          explode($"rules") as "used_properties"
        )

        // writing data to corresponding date
        propertyUsageData.write.mode(SaveMode.Overwrite).parquet(propertyUsageRawPath + "dt=" + reqDt)

      }

    })

    // Work
    // Once we have usage data for all dates, we begin to compute, first_usage_date, last_usage_date
    // by examining the schema of flatTable of features (in prod) over date), we get the first_created_date and last_created_date

    val featFlatTablePath = "s3a://midgar-aws-workspace/prod/mapfeatures/features/1/flatTable/"
    // Getting list of feature names which are available currently
    val featureLatestDateStr = getLatestTableDate(featFlatTablePath, spark, dt.toString(ArgsUtils.formatter), "dt=").getOrElse(null)
    val currFeatCols = spark.read.parquet(featFlatTablePath + "dt=" + featureLatestDateStr).columns
    val currAvlProperties = spark.sparkContext.parallelize(currFeatCols).toDF("property_name")

    val allAvlJoinedFeatureDates = getAvlTableDates(featFlatTablePath, spark, "dt=")

    val flatTableRawDf = allAvlJoinedFeatureDates.map(dtt => {
      val flatTablCols = spark.read.parquet(featFlatTablePath + "dt=" + dtt).columns
      spark.sparkContext.parallelize(flatTablCols).toDF("used_properties").withColumn("feat_date", lit(dtt))
    }).reduce(_ union _).cache()

    val flatTableDf = flatTableRawDf.groupBy($"used_properties")
      .agg(
        min($"feat_date") as "feat_first_created_date",
        max($"feat_date") as "feat_last_created_date"
      )
      .withColumn("feat_first_created_date", $"feat_first_created_date".cast("date"))
      .withColumn("feat_last_created_date", $"feat_last_created_date".cast("date"))

    val featUsageTableRawDf = spark.read.parquet(propertyUsageRawPath).cache()

    val featUsageTableDf = featUsageTableRawDf.groupBy($"used_properties")
      .agg(
        min($"first_usage_at") as "first_usage_date",
        max($"next_usage_at") as "latest_usage_date"
      )
      .withColumn("first_usage_date", $"first_usage_date".cast("date"))
      .withColumn("latest_usage_date", $"latest_usage_date".cast("date"))

    val joinedFeatDf = flatTableDf.join(featUsageTableDf, Seq("used_properties"), "left_outer")
      .withColumn("curr_date", lit(featureLatestDateStr).cast("date"))

    // We create two columns here, days_since_first_created, days_since_last_used
    // if curr_date

    // if first_usage_date is null ~ [Unused] properties
    // if feat_last_created_date - curr_date < 0 [New] properties, else [Deprecated]
    // For
    // if days_since_created > 60, days_since_used > 60 ~ [Stale], we call them "Stale" properties, staleThrehold is parametrised
    // feat_last_created_date

    val newFeatureThreshold = 7
    val staleFeatureThreshold = 30

    val reportFeatDf = joinedFeatDf
      .withColumn("deprecated_days", datediff($"curr_date", $"feat_last_created_date"))
      .withColumn("stale_days", datediff($"curr_date", $"latest_usage_date"))
      .withColumn("deprecated_flag", when(
        $"deprecated_days".gt(lit(0)),
        "Deprecated"
      ).otherwise("InProd"))
      .withColumn("newness_flag", when(
        datediff($"curr_date", $"feat_first_created_date").leq(lit(newFeatureThreshold)),
        "NewProperty"
      ).otherwise("OldProperty"))
      .withColumn(
        "stale_flag",
        when(
          ($"deprecated_flag" === "InProd") &&
            ($"newness_flag" === "OldProperty") &&
            ($"stale_days".isNotNull) &&
            ($"stale_days".geq(lit(staleFeatureThreshold))),
          "Stale"
        )
          .when(
            ($"deprecated_flag" === "InProd") &&
              ($"newness_flag" === "OldProperty") &&
              ($"stale_days".isNotNull) &&
              ($"stale_days".leq(lit(staleFeatureThreshold - 1))),
            "NotStale"
          )
          .when(
            ($"deprecated_flag" === "InProd") &&
              ($"newness_flag" === "OldProperty") &&
              ($"stale_days".isNull),
            "NeverUsedOldProperty"
          )
          .when(
            ($"deprecated_flag" === "InProd") &&
              ($"newness_flag" === "NewProperty") &&
              ($"stale_days".isNull),
            "NeverUsedNewProperty"
          )
      )

    reportFeatDf.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(propertyUsageReportPath + "dt=" + targetDate)

  }

}

