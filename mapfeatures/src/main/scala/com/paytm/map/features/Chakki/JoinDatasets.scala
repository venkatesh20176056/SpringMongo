package com.paytm.map.features.Chakki

import com.paytm.map.features.Chakki.FeatChakki.Constants.execLevel2WritePath
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.featuresplus.genderestimation.GEModelConfigs.featureName
import com.paytm.map.featuresplus.rechargeprediction.Constants.packageName
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import com.paytm.map.featuresplus.superuserid.Constants.{packageName => superUserPackage}

object JoinDatasets extends JoinDatasetsJob with SparkJob with SparkJobBootstrap

trait JoinDatasetsJob {
  this: SparkJob =>

  val JobName = "JoinDatasets"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def dropNullValues(column: Column): Column = {
    val isNotNullCondition = udf((value: Any) =>
      value match {
        case v: Long if v == 0             => false
        case v: Double if v == 0           => false
        case v: Int if v == 0              => false
        case v: Float if v == 0            => false
        case v: Iterable[Any] if v.isEmpty => false
        case v if v == null                => false
        case _                             => true
      })

    when(isNotNullCondition(column), column).otherwise(null)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings.featuresDfs
    import spark.implicits._
    val groupPrefix = args(1)

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val featureLevelsPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.level}"
    val baseLevelsTablePath = settings.featuresDfs.baseDFS.aggPath

    val profilePath = s"${featuresDfs.featuresTable}/${featuresDfs.profile}dt=$targetDateStr"
    val customerSegmentPath = featuresDfs.featuresTable + groupPrefix + s"/ids/dt=$targetDateStr"

    val outPath = s"${featuresDfs.featuresTable}/$groupPrefix/${featuresDfs.joined}"
    val checkpointBasePath = s"$featureLevelsPath/checkpoint"
    val featuresCheckpointPath = s"$checkpointBasePath/joined_df/$groupPrefix/features/"

    val profile = spark.read.parquet(profilePath)
      .filter(isAllNumeric($"customer_id"))

    val customerSegment = spark.read.parquet(customerSegmentPath)
      .filter(isAllNumeric($"customer_id"))

    val profileNameCols = Set("first_name", "full_name")

    if (groupPrefix != "4") { //TODO: Better way to check instead of hardcoding?

      // Level identifiers of features that are aggregated at feature agg level
      val featureLevels = Seq(
        gaL3EC2, gaL3BK, gaBK, gaL2BK, gaL2RU, s"${gaL3EC4}_I",
        gaL3EC1, gaL3EC3, gaL3RU, gaL2EC, gaL1, s"${gaL3EC4}_II", s"${gaL3EC4}_III",
        s"L3${l2EC}4_I", s"L3${l2EC}1", s"L3${l2EC}2_I", wallet, l2RU,
        s"L3${l2EC}4_II", s"L3$l2RU", s"L3${l2EC}2_II", payments, online, l2BK, bank,
        s"L3${l2EC}4_III", s"L3$l2BK", s"L3${l2EC}3", l2EC, l1, sms, lmsPostpaid, lmsPostpaidV2, gold, gaGold,
        superCashback, upi, lastSeen, Push, gamepind, postpaid, gaFS, medical, device, location, insurance, signal, paytmFirstDropped, overall
      )

      //TODO: Make it a part of Chakki instead of hardcoding?
      // Level identifiers of features that are aggregated at base table level (base features)
      val baseLevels = Seq(bills, postPaid, distanceAndSpeed, customerDevice)

      // Paths for all features
      val basePaths = baseLevels.map(level => s"$baseLevelsTablePath/$level/dt=$targetDateStr/")
      val featurePaths = featureLevels.map(level => s"$featureLevelsPath/$level/dt=$targetDateStr")

      // Read base features and join it with customerSegment to get relevant customers for the group
      val baseFeatureDF = readPathsAll(spark, basePaths)
        .map(_.filter(isAllNumeric($"customer_id")).where($"customer_id".isNotNull).dropDuplicates("customer_id"))
        .joinAll(Seq("customer_id"), "outer", withRepartition = false)
        .join(customerSegment, Seq("customer_id"), "left_semi")

      // Read features
      val featureDFs = readPathsAll(spark, featurePaths)
        .map(_.filter(isAllNumeric($"customer_id")).where($"customer_id".isNotNull))
      // Features along with their level identifier
      val featuresWithLevels = featureDFs.zip(featureLevels)

      // Compress non-null values only to a json
      val compressedFeatureDFs = featuresWithLevels.map {
        case (df, execLevel) =>
          val cols = df.columns.diff(Seq("customer_id"))
          val nonNullCols = cols.map(colName => dropNullValues(col(colName)) as colName)

          df.select(
            $"customer_id",
            to_json(struct(nonNullCols: _*)) as s"${execLevel}_features"
          )
      }

      val modelLevels = Seq(featureName, packageName, superUserPackage, "weighted_trendfiltering")
      val modelFeaturePath = featuresDfs.featuresPlusFeatures

      val modelFeaturesPaths = modelLevels.map { level =>
        println(s"$modelFeaturePath/$level")
        val latestFeatureDate = getLatestTableDate(s"$modelFeaturePath/$level", spark, targetDateStr, "dt=")
        println(latestFeatureDate)
        if (latestFeatureDate.isDefined) s"$modelFeaturePath/$level/dt=${latestFeatureDate.get}"
        else ""
      }.filter(_.nonEmpty)

      val modelFeatures = readPathsAll(spark, modelFeaturesPaths)
        .map(_.filter(isAllNumeric($"customer_id")).where($"customer_id".isNotNull).dropDuplicates("customer_id"))
        .joinAll(Seq("customer_id"), "outer", withRepartition = false)

      // Join compressed features, base features and profile together
      val joinedDF = compressedFeatureDFs
        .joinAll(Seq("customer_id"), "outer", withRepartition = false)
        .join(baseFeatureDF, Seq("customer_id"), "outer")
        .join(profile, Seq("customer_id"), "left_outer")
        .join(modelFeatures, Seq("customer_id"), "left_outer")
        .repartition($"customer_id")

      // Expand the json to a struct
      val jsonToStructCols = featuresWithLevels.map {
        case (df, execLevel) =>
          from_json(
            col(s"${execLevel}_features"),
            StructType(df.schema.filter(_.name != "customer_id"))
          ).as(s"${execLevel}_features")
      }
      // Expand the struct to individual columns
      val structToIndividualCols = featureLevels.map(level => col(s"${level}_features.*"))
      val otherCols = profile.columns.map(col) ++
        baseFeatureDF.columns.diff(Seq("customer_id")).map(col) ++
        modelFeatures.columns.diff(Seq("customer_id")).map(col)

      val flatFeaturesDF = joinedDF
        .select(otherCols ++ jsonToStructCols: _*)
        .select(otherCols ++ structToIndividualCols: _*)

      // Checkpoint expanded features to break lineage
      flatFeaturesDF.saveParquet(featuresCheckpointPath, withRepartition = false)

      // Read back expanded features
      val featuresDF = spark.read.parquet(featuresCheckpointPath)
      // Get the columns which need to be dropped
      val dropCols: Seq[String] = getDropCols(featuresDF.columns)
      // Since dropping multiple columns if inefficient, select the ones required
      val selectCols = featuresDF.columns.diff(dropCols)

      // Select required columns and do some post processing
      val allFeaturesDF = featuresDF
        .select(selectCols.head, selectCols.tail: _*)
        .castToCorrectType
        .na.drop(Seq("customer_id"))
        .lowerStrings(colsToSkip = profileNameCols)
        .replaceEmptyStrings

      allFeaturesDF.saveParquet(s"$outPath/dt=$targetDateStr", withRepartition = false)
    } else {

      //Reds Group1 to force Group4 to have correct schema
      val group1SchemaPath = s"${featuresDfs.featuresTable}/1/${featuresDfs.joined}/dt=$targetDateStr"
      val allGroup1Fields = spark.read.parquet(group1SchemaPath).schema.fields

      // For group 4 just profile features are needed
      profile.join(customerSegment, Seq("customer_id"), "left_semi")
        .castToCorrectType
        .na.drop(Seq("customer_id"))
        .lowerStrings(colsToSkip = profileNameCols)
        .replaceEmptyStrings
        .addNumericColumnsFromGroup1(allGroup1Fields, spark)
        .saveParquet(s"$outPath/dt=$targetDateStr", 1000)
    }
  }

  private def getSingleRowDF(columnsFromGroup1: Seq[StructField], spark: SparkSession) = {
    val zeroValues = columnsFromGroup1
      .map(column => column.dataType.typeName match {
        case _ => null
      })

    val row = Row.fromSeq(zeroValues)
    val rdd = spark.sparkContext.parallelize(Seq(row))
    spark.createDataFrame(rdd, StructType(columnsFromGroup1))
  }

  def isNumericColumn(dataType: DataType) = (dataType == DoubleType) || (dataType == LongType) || (dataType == IntegerType)

  implicit class JoinImplicits(df: DataFrame) {
    def addNumericColumnsFromGroup1(allGroup1Fields: Seq[StructField], spark: SparkSession) = {
      val fieldsToAdd = allGroup1Fields.diff(df.schema.fields)
        .filter(field => isNumericColumn(field.dataType))

      val singleRowGroup1DF = getSingleRowDF(fieldsToAdd, spark)

      df.crossJoin(broadcast(singleRowGroup1DF))
    }
  }
}
