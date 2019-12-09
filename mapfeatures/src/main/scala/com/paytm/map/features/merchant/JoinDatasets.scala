package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.joda.time.DateTime

object JoinDatasetsJob extends JoinDatasets with SparkJob with SparkJobBootstrap

trait JoinDatasets {
  this: SparkJob =>

  val JobName = "MerchantJoin"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)

    val profilePath = settings.featuresDfs.baseDFS.aggPath + "/MerchantProfile"

    val outPath = s"${settings.featuresDfs.featuresTable}${settings.featuresDfs.merchantFlatTable}/dt=${targetDateStr}"

    val featuresPaths = Seq("MS", "MSC", "MGA")
      .map(level => s"${settings.featuresDfs.featuresTable}/${settings.featuresDfs.level}/$level/dt=${targetDateStr}")

    val profileDF = spark.read.parquet(s"$profilePath/dt=${targetDateStr}").filter("merchant_id is not null")
    val featureDFs = featuresPaths.map(spark.read.parquet(_).filter("merchant_id is not null"))

    val finalDF = (Seq(profileDF) ++ featureDFs).joinAll(Seq("merchant_id"), "left_outer", 200)
      .castDateToString
      .fillNulls

    finalDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outPath)

  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  implicit class MerchantImplicit(df: DataFrame) {
    def castDateToString(): DataFrame = {
      val castStatement = df.schema.map(column => {
        val castType = column.dataType match {
          case DateType      => StringType
          case TimestampType => StringType
          case _             => column.dataType
        }
        col(column.name).cast(castType)
      })
      df.select(castStatement: _*)
    }

  }

}

