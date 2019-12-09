package com.paytm.map.features.Chakki.FeatChakki

import com.paytm.map.features.Chakki.FeatChakki.Constants._
import com.paytm.map.features.Chakki.Features.TimeSeries._
import com.paytm.map.features.Chakki.Features._
import org.apache.spark.sql._

import scala.util.Try

object ResolveFeatures {
  def apply(spark: SparkSession, FeatListPath: String, SpecSheetPath: String, execLevel: String) =
    new ResolveFeatures(spark, FeatListPath, SpecSheetPath, execLevel)
}

class ResolveFeatures(spark: SparkSession, FeatListPath: String, SpecSheetPath: String, execLevel: String) {

  import spark.implicits._

  /**
   * Extract the logical level from the execution level
   */
  val level: String = execLevel2Level(execLevel)

  /**
   * Parse the specsheet for an logical group = level
   */
  lazy val specSheet: Seq[Row] = {
    spark.read.option("mergeSchema", value = true).json(SpecSheetPath)
      .where($"varName".isNotNull)
      .where($"level" === level)
      .collect()
  }

  /**
   * Parse the required feature the execution stage/level specified
   */
  lazy val reqFeatures: Set[String] = {
    spark.read.option("header", value = true).csv(FeatListPath)
      .where($"isEnable" === 1)
      .where($"ExecLevel" === execLevel)
      .collect()
      .map(_.getAs[String]("Name"))
      .toSet
  }

  /**
   * Required Features name and ts info splitted
   */
  private lazy val reqVarNameTs = reqFeatures.map(getVarNameTs).map { case (varName, ts) => (varName, ts, ts) }

  /**
   * Parse the dependent feature for POST_UDF functions and enable these for calculation
   * And Further dependencies of those function as UDAF OR UDWF
   */
  lazy val dependentFeatures: Set[String] = reqVarNameTs.flatMap { case (varName, ts, isFinal) => getDependentFeatures(varName, ts, isFinal) }

  /**
   * Dependent Features name and ts info splitted
   */
  private lazy val dependentVarNameTs = dependentFeatures.map(getVarNameTs).map { case (varName, ts) => (varName, ts, null) }

  /**
   * Final Featureset to be executed by executor.
   */
  lazy val featureSet: Seq[Feat] = {
    val allFeatures = reqVarNameTs ++ dependentVarNameTs
    println("All features : \n", allFeatures)
    allFeatures
      .groupBy { case (varName, _, _) => varName }
      .map {
        case (varName, valueSet) =>
          val tsSet = valueSet.map { case (_, ts, _) => ts }.toSeq
          val isFinalSet = valueSet.map { case (_, _, isFinal) => isFinal }.filter(_ != null).toSeq
          val featSpec = specSheet.filter(_.getAs[String]("varName") == varName)
          println("featspec", featSpec)
          assert(featSpec.nonEmpty, s"Feature $varName has no specs in specsSheet!!!")
          row2Feat(varName, tsSet, featSpec.head, isFinalSet)
      }.toSeq
  }

  lazy val baseFeaturesRequired: Set[String] = {
    featureSet.flatMap {
      case feat: UDFFeat if feat.stage == PRE_UDF => feat.dependency
      case feat: UDAFFeat                         => feat.dependency.diff(preUDFSpecs)
      case feat: UDWFFeat                         => feat.dependency.diff(preUDFSpecs)
      case _                                      => Seq.empty[String]
    }.toSet
  }

  /**
   * Split the column name into base variable + ts information
   *
   * @param featName : feature name
   * @return : tuple of varName,ts.
   */
  private def getVarNameTs(featName: String): (String, String) = (getParent(featName), getTimeSeries(featName))

  private val preUDFSpecs = {
    if (specSheet.headOption.exists(_.schema.fieldNames.contains("stage"))) {
      specSheet.filter(_.getAs[String]("stage") == "PRE_UDF").map(_.getAs[String]("varName"))
    } else {
      Seq[String]()
    }
  }

  private def getDependentFeatures(varName: String, ts: String, isFinal: String): Seq[String] = {
    val featSpec = specSheet.filter(_.getAs[String]("varName") == varName)
    assert(featSpec.nonEmpty, s"Feature $varName has no specs in specsSheet!!!")
    val feat = row2Feat(varName, Seq(ts), featSpec.head, Seq(isFinal))
    feat match {
      case feat1: UDFFeat if feat1.stage == POST_UDF => {
        val postUDFDependency = feat.dependency
        val secondLevelDependency = postUDFDependency.map(getVarNameTs).flatMap { case (varName1, ts1) => getDependentFeatures(varName1, ts1, "") }
        postUDFDependency ++ secondLevelDependency
      }
      case x: UDAFFeat => feat.dependency.intersect(preUDFSpecs)
      case x: UDWFFeat => feat.dependency.intersect(preUDFSpecs)
      case _           => Seq.empty[String]
    }
  }

  /**
   * Check whether the generated feature set passes checks like
   * All Required Columns are Generated
   * All Generated Columns are calculated
   * All dependent columns are also calculated
   */
  lazy val isValid: Boolean = {

    val generatedCol = featureSet.flatMap { feat =>
      feat.finalTs.map(ts => feat match {
        case feat1: UDWFFeat => feat1.getTsUDAFColName(ts)
        case _               => feat.getTsColName(ts)
      })
    }.toSet

    val calculatedCol = featureSet.flatMap { feat =>
      feat.timeAgg.map(ts => feat match {
        case feat1: UDWFFeat => feat1.getTsUDAFColName(ts)
        case _               => feat.getTsColName(ts)
      })
    }.toSet

    // check whether all requiredColumns are generated
    val reqCheck = reqFeatures.subsetOf(generatedCol)
    if (!reqCheck) println(reqFeatures.diff(generatedCol).mkString(","))
    assert(reqCheck, "Required Columns Check Failed!!")

    //Check Whether all generated Columns are Calculated
    val isFinalCheck = generatedCol.subsetOf(calculatedCol)
    if (!isFinalCheck) println(generatedCol.diff(calculatedCol).mkString(","))
    assert(isFinalCheck, "isFinal Check Failed!!")

    // Also Check whether All POST UDF dependencies are adhered
    val dependencyCheck = dependentFeatures.subsetOf(calculatedCol)
    if (!dependencyCheck) println(dependentFeatures.diff(calculatedCol).mkString(","))
    assert(dependencyCheck, "Dependency Check Failed!!")

    reqCheck && isFinalCheck && dependencyCheck
  }

}