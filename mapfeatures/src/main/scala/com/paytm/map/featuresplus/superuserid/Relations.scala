package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.superuserid.Constants._
import com.paytm.map.featuresplus.superuserid.Utility.{DFFunctions, Functions, UDF}
import com.paytm.map.featuresplus.superuserid.DataUtility._

object Relations extends RelationsJobs with SparkJobBootstrap with SparkJob

trait RelationsJobs {
  this: SparkJob =>

  val JobName = "BuildingRelations"

  /**
   * Build relations from the source tables for each feature and join profile features as well
   */

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    /** arguments **/
    val date = ArgsUtils.getTargetDate(args)
    val dateStr = ArgsUtils.getTargetDateStr(args)
    import settings._
    implicit val implicitSpark: SparkSession = spark
    val Path = new PathsClass(settings.featuresDfs.featuresPlusRoot)

    /** Work **/

    val relationSeq: Seq[DataFrame] = strongFeaturesOrder.map { feature =>
      println(feature)
      val featureCol = featuresConfigMap(feature).featureName
      val frequencyCol = featuresConfigMap(feature).frequency
      val similarCol = featuresConfigMap(feature).similarity
      val featureWritePath = s"${Path.baseTablePath}/${featuresConfigMap(feature).path}"

      val aggregations = spark.read.parquet(s"$featureWritePath/dt=$dateStr")
      val relations = getRelations(aggregations, feature)

      val dedupedRelations = relations.groupBy(identifierCols.map(r => col(r)): _*).agg(
        collect_set(featureCol) as featureCol,
        sum(s"${frequencyCol}_1") as s"${frequencyCol}_1",
        sum(s"${frequencyCol}_2") as s"${frequencyCol}_2",
        sum(s"$similarCol") as s"$similarCol"
      )
      dedupedRelations
    }

    val allStrongRelations = relationSeq.
      reduce((accu: DataFrame, elem: DataFrame) =>
        accu.join(elem, Seq(s"${customerIdCol}_1", s"${customerIdCol}_2"), "outer"))

    val allRelationsColumns: Array[Column] =
      (allStrongRelations.columns.map(r => col(r)) ++
        (1 to 2).flatMap(r => profileColumnsMap.keySet.toSeq.map(i => col(s"${i}_$r"))) ++
        profileColumnsMap.keySet.toSeq.map(item => profileColumnsMap(item)))

    val profileBasePath = Path.productionPath(s"${featuresDfs.featuresTable}/${featuresDfs.profile}")

    val latestProfilePath = s"$profileBasePath/dt=${Functions.getLatestDate(profileBasePath, dateStr)}"

    val profile = readProfile(latestProfilePath)

    val relationWithProfile = allStrongRelations.
      join(
        profile.select(profile.columns.map(r => col(r).as(s"${r}_1")): _*),
        Seq(s"${customerIdCol}_1"), "left"
      ).
        join(
          profile.select(profile.columns.map(r => col(r).as(s"${r}_2")): _*),
          Seq(s"${customerIdCol}_2"), "left"
        )

    val allRelations = relationWithProfile.select(allRelationsColumns: _*)

    allRelations.write.mode(SaveMode.Overwrite).parquet(s"${Path.allRelationsPath}/dt=$dateStr")
    // For v2; can be made partitionedBy; even better maintain a updated_date to partition for space efficient storing

    /**
     * relation has schema similar to this
     * root
     * |-- customer_id_1: long (nullable = true)
     * |-- customer_id_2: long (nullable = true)
     * |-- delivery_phone_frequency_1: long (nullable = true)
     * |-- delivery_phone_frequency_2: long (nullable = true)
     * |-- delivery_phone: string (nullable = true)
     * |-- delivery_phone_similarity: long (nullable = true)
     * |-- account_no_frequency_1: long (nullable = true)
     * |-- account_no_frequency_2: long (nullable = true)
     * |-- account_no: string (nullable = true)
     * |-- account_no_similarity: long (nullable = true)
     * |-- KYC_completion_1: integer (nullable = true)
     * |-- email_id_1: string (nullable = true)
     * |-- wallet_balance_1: double (nullable = true)
     * |-- age_1: integer (nullable = true)
     * |-- latest_lang_1: string (nullable = true)
     * |-- sign_up_date_1: timestamp (nullable = true)
     * |-- is_email_verified_1: string (nullable = true)
     * |-- gender_1: string (nullable = true)
     * |-- KYC_completion_2: integer (nullable = true)
     * |-- email_id_2: string (nullable = true)
     * |-- wallet_balance_2: double (nullable = true)
     * |-- age_2: integer (nullable = true)
     * |-- latest_lang_2: string (nullable = true)
     * |-- sign_up_date_2: timestamp (nullable = true)
     * |-- is_email_verified_2: string (nullable = true)
     * |-- gender_2: string (nullable = true)
     * |-- KYC_completion_similarity: integer (nullable = false)
     * |-- email_id_similarity: integer (nullable = false)
     * |-- wallet_balance_similarity: double (nullable = true)
     * |-- age_similarity: integer (nullable = false)
     * |-- latest_lang_similarity: integer (nullable = false)
     * |-- sign_up_date_similarity: integer (nullable = false)
     * |-- is_email_verified_similarity: integer (nullable = false)
     * |-- gender_similarity: integer (nullable = false)
     */
  }

  def getRelations(baseData: DataFrame, feature: String): DataFrame = {
    val featureCol = featuresConfigMap(feature).featureName
    val frequencyCol = featuresConfigMap(feature).frequency
    val featureSimilarityCol = featuresConfigMap(feature).similarity
    val featureThreshold = featuresConfigMap(feature).threshold

    val base = baseData.select(customerIdCol, featureCol, frequencyCol)

    val filterIds = base.
      groupBy(featureCol).
      agg(count(customerIdCol) as "customerCount").
      filter(col("customerCount") between (1, 5000)).
      select(featureCol)

    val baseCleaned = base.join(filterIds, Seq(featureCol), "left_semi")

    val relations = baseCleaned.
      select(DFFunctions.addToColumnName(baseCleaned.columns, "_1"): _*).
      join(
        baseCleaned.select(DFFunctions.addToColumnName(baseCleaned.columns, "_2"): _*),
        col(s"${featureCol}_1") === col(s"${featureCol}_2") &&
          col(s"${customerIdCol}_1") =!= col(s"${customerIdCol}_2"),
        joinType = "inner"
      ).
        drop(col(s"${featureCol}_2")).withColumnRenamed(s"${featureCol}_1", featureCol).
        withColumn(featureSimilarityCol, UDF.minSimilarity(col(s"${frequencyCol}_1"), col(s"${frequencyCol}_2"))).
        withColumn("smallId", UDF.minSimilarity(col(s"${customerIdCol}_1"), col(s"${customerIdCol}_2"))).
        filter(col(s"${customerIdCol}_1") === col("smallId")).drop(col("smallId")). // deduplication
        filter(col(featureSimilarityCol) geq featureThreshold)

    relations
  }

}
