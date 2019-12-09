package com.paytm.map.features.sanity

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.monitoring.Monitoring
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs.gaugeValuesWithTags
import com.paytm.map.features.base.Constants.gaDelayOffset
import com.paytm.map.features.datasets.ReadBaseTable.levelNameToID
import com.paytm.map.features.datasets.Constants._
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

object CheckRawDatasetFreshness extends CheckRawDatasetFreshnessJob with SparkJob with SparkJobBootstrap

trait CheckRawDatasetFreshnessJob extends Monitoring {
  this: SparkJob =>

  val JobName = "CheckRawDatasetFreshness"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)

    // Prod Base Table path
    val baseTableBasePath = s"${settings.featuresDfs.baseDFS.aggPath}/".replace("/stg/", "/prod/")

    // For new levelPrefixes add them to Constants file and refer here
    val levelPrefixesToCheck = Seq(l1, l2RU, l2EC, l2BK, s"L3$l2RU", s"L3$l2BK", s"L3${l2EC}1", s"L3${l2EC}2",
      payments, online, wallet, bank, bills)

    // For new ga levelPrefixes add them to Constants file and refer here
    val gaLevelPrefixesToCheck = Seq(gaL1, gaL2RU, gaL2BK, gaL2EC, gaBK, gaL3RU, gaL3BK, gaL3EC1, gaL3EC2)

    val allPrefixesToCheck = levelPrefixesToCheck ++ gaLevelPrefixesToCheck

    allPrefixesToCheck.foreach {
      levelPrefix =>
        val levelID = levelNameToID(levelPrefix)

        val levelAvlDate = DateTime.parse(getLatestTableDate(
          baseTableBasePath + levelID,
          spark,
          targetDate.toString(ArgsUtils.formatter),
          "dt=",
          "part-00000*"
        ).orNull)

        val levelDataDogMetric = s"RawDataFreshness.numOfDaysOld"
        val levelDateDiff = Days.daysBetween(levelAvlDate, targetDate).getDays

        println(s"$levelPrefix : ${levelDateDiff.toString} Days")

        val threshold = if (levelID.contains("GAFeatures")) gaDelayOffset else 0
        if (levelDateDiff > threshold) {
          gaugeValuesWithTags(
            metricName  = levelDataDogMetric,
            metricValue = levelDateDiff,
            tagMap      = Map("level" -> levelPrefix)
          )
        }
    }
  }
}