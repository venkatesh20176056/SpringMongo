package com.paytm.map.features.datasets.hero

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MetadataBuilder, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteRedshift extends WriteRedshiftJob with SparkJob with SparkJobBootstrap

trait WriteRedshiftJob {
  this: SparkJob =>

  val JobName = "WriteRedshift"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.{featuresDfs, redshiftDb}

    val groupPrefix = args(1)
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    /** Features path and columns to drop */
    val featuresPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.joined}/dt=$targetDateStr"
    val personalInfoCols = getPICols(groupPrefix.toString)

    /** Redshift URL and table paths */
    val rsUrl = getRedshiftUrl(settings)
    val rsSuffix = getRedshiftSuffix(groupPrefix.toString)
    val rsTempTable = s"${featuresDfs.featuresTemp}/$rsSuffix/"
    val rsFeatures = s"${redshiftDb.features}_$rsSuffix"

    /** Read most recent features flat table for the group, drop personal info fields */
    val features = spark.read.parquet(featuresPath)
      .drop(personalInfoCols: _*)
      .withColumn("updated_date", lit(targetDateStr).cast("date"))

    /** Specify redshift data types to ensure that enough space is allowed for long list-string fields (VARCHAR(MAX))*/
    val featuresSchemaMap = features.schema.map { feature =>
      val redshiftType = feature.dataType match {
        case StringType      => "VARCHAR(256)"
        case LongType        => "BIGINT"
        case IntegerType     => "INTEGER"
        case ArrayType(_, _) => "VARCHAR(MAX)"
        case DoubleType      => "DOUBLE PRECISION"
        case TimestampType   => "TIMESTAMP"
        case DateType        => "DATE"
        case _               => null
      }
      feature.name -> redshiftType
    }.toMap

    /** Transform Array columns to string, list entries separated by ; */
    val featuresSql = features
      .listColsToString

    val featuresMeta = featuresSql
      .addMetadata(featuresSchemaMap)

    /** Save table - Databricks creates temp Avro table and then writes to redshift DB */
    featuresMeta.saveRedshift(rsUrl, rsFeatures, rsTempTable, "overwrite")
  }

  def getRedshiftSuffix(groupPrefixString: String): String = {
    groupPrefixString match {
      case "hero" => s"canada"
      case _      => s"group$groupPrefixString"
    }
  }

  def getPICols(groupPrefixString: String): Seq[String] = {
    groupPrefixString match {
      case "hero" => Seq("email_id", "email", "first_name", "last_name", "number")
      case _      => Seq("email_id", "phone_no")
    }
  }

  def getRedshiftUrl(settings: Settings): String = {
    import settings._
    val prefixEnv = s"${awsCfg.parameterStore.pathPrefix}/$environment"

    val rsUsername = redshiftDb.user
    val rsPassword = getParameter(redshiftDb.passwordPath, prefixEnv)
    val rsHostname = redshiftDb.host
    val rsPort = redshiftDb.port
    val rsDb = redshiftDb.db
    s"jdbc:redshift://$rsHostname:$rsPort/$rsDb?user=$rsUsername&password=$rsPassword"
  }

  implicit class UpdatedDataFrame(df: DataFrame) {

    def addMetadata(schema: Map[String, String]): DataFrame = {
      val cols = schema.map {
        case (colName, colType) =>
          val metadata = new MetadataBuilder().putString("redshift_type", colType).build()
          df(colName) as (colName, metadata)
      }.toSeq
      df.select(cols: _*)
    }
  }
}

