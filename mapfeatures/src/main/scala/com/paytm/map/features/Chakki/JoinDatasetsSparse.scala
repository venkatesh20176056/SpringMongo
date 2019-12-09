package com.paytm.map.features.Chakki

import com.paytm.map.features.Chakki.FeatChakki.Constants.execLevel2WritePath
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object JoinDatasetsSparse extends JoinDatasetsSparseJob with SparkJob with SparkJobBootstrap

trait JoinDatasetsSparseJob {
  this: SparkJob =>

  val JobName = "JoinDatasetsSparse"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    val groupPrefix = args(1)

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val featureLevelsPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}"
    val baseLevelsTablePath = settings.featuresDfs.baseDFS.aggPath

    val profilePath = s"${featuresDfs.featuresTable}/${featuresDfs.profile}dt=$targetDateStr"
    val customerSegmentPath = featuresDfs.featuresTable + groupPrefix + s"/ids/dt=$targetDateStr"

    val outPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.joined}"

    val profile = spark.read.parquet(profilePath)
    val customerSegment = spark.read.parquet(customerSegmentPath)

    if (groupPrefix != "4") { //TODO: Better way to check instead of hardcoding?

      // Level identifiers of features that are aggregated at feature agg level
      val execLevels = Seq(
        "BANK", "BK", "EC", "GABK", "GAL2BK", "GAL2EC", "GAL2RU", "GAL3BK", "GAL3EC1", "GAL3EC2", "GAL3EC3", "GAL3EC4_I",
        "GAL3EC4_II", "GAL3EC4_III", "GAL3RU", "GAPAYTM", "L3BK", "L3EC1", "L3EC2_I", "L3EC2_II", "L3EC3", "L3EC4_I",
        "L3EC4_II", "L3EC4_III", "L3RU", "LMS", "PAYMENTS", "ONLINE", "PAYTM", "RU", "WALLET"
      )
      // Level identifiers of features that are aggregated at base table level (base features)
      val baseLevels = Seq("Bills")

      // Paths for all features
      val basePaths = baseLevels.map(level => s"$baseLevelsTablePath/$level/dt=$targetDateStr/")
      val featurePaths = execLevels.map(execLevel => s"$featureLevelsPath/${execLevel2WritePath(execLevel)}/dt=$targetDateStr")

      // Read base features and join it with customerSegment to get relevant customers for the group
      // Note: Its broadcasted currently as its small enough
      val baseFeatureDF = readPathsAll(spark, basePaths)
        .joinAll(Seq("customer_id"), "outer", withRepartition = false)
        .join(customerSegment, Seq("customer_id"), "left_semi")
        .toSparseDF

      // Read features and broadcast those which are small
      val featureDFs = readPathsAll(spark, featurePaths)
      // Features along with their level identifier
      val featuresWithLevels = featureDFs.zip(execLevels)

      // Compress non-null values only to a json
      val compressedFeatureDFs = featuresWithLevels.map { case (df, _) => df.toSparseDF }

      // Join compressed features, base features and profile together
      val joinedDF = compressedFeatureDFs
        .joinAll(Seq("customer_id"), "outer", withRepartition = false)
        .join(baseFeatureDF, Seq("customer_id"), "outer")
        .join(profile, Seq("customer_id"), "left_outer")

      // Checkpoint expanded features to break lineage
      joinedDF.saveParquet(s"$outPath/dt=$targetDateStr", withRepartition = false)
    } else {
      // For group 4 just profile features are needed
      profile.join(customerSegment, Seq("customer_id"), "left_semi")
        .lowerStrings()
        .replaceEmptyStrings
        .saveParquet(s"$outPath/dt=$targetDateStr", 1000)
    }
  }
}
