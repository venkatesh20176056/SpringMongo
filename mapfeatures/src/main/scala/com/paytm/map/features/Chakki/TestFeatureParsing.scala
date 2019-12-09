package com.paytm.map.features.Chakki

import com.paytm.map.features.Chakki.FeatChakki.Constants._
import com.paytm.map.features.Chakki.FeatChakki.{FeatExecutor, FeatParser, ResolveFeatures}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.LocalDate

object TestFeatureParsing {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    run(spark, args)
  }

  def run(spark: SparkSession, args: Array[String]): Unit = {

    // Read Arguments
    val levelPrefix = args(0)
    val isLog = if (args.length > 1) args(1).toInt == 1 else false
    //val levelPrefix = Seq("BANK", "BK", "EC", "GABK", "GAL2BK", "GAL2EC", "GAL2RU", "GAL3BK", "GAL3EC1", "GAL3EC2",
    //  "GAL3EC3", "GAL3EC4_I", "GAL3EC4_II", "GAL3EC4_III", "GAL3RU", "GAPAYTM", "L3BK", "L3EC1", "L3EC2_I", "L3EC2_II",
    //  "L3EC3", "L3EC4_I", "L3EC4_II", "L3EC4_III", "L3RU", "LMS", "PAYMENTS", "ONLINE", "PAYTM", "RU", "WALLET").mkString(",")
    //    val SpecSheetPath = "/Users/pawan/IdeaProjects/map-features/azkaban/resources/SpecSheets/" //args(1)
    //    val FeatListPath = "/Users/pawan/IdeaProjects/map-features/azkaban/resources/feature_list_india_new.csv/" //args(2)
    val SpecSheetPath = "/home/venkateshakula/Documents/projects/map-features/azkaban/resources/SpecSheets/"
    val FeatListPath = "/home/venkateshakula/Documents/projects/map-features/azkaban/resources/feature_list_india_new.csv"

    //Transform to Features
    val execLevels = levelPrefix.split(",")
    execLevels.foreach { execLevel =>
      val resolvedFeatures = ResolveFeatures(spark, FeatListPath, SpecSheetPath, execLevel)
      assert(resolvedFeatures.isValid)
      // print feature Specs
      if (isLog) resolvedFeatures.featureSet.foreach(println)

      //check if we have schema
      val level = execLevel2Level(execLevel)
      if (level2Schema.contains(level)) {
        // Make Empty DataFrame
        val schema = StructType(level2Schema(level))
        val baseFeatures = (resolvedFeatures.baseFeaturesRequired ++ Set("customer_id", "dt")).toSeq
        val emptyBaseDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          .select(baseFeatures.head, baseFeatures.tail: _*)
        val todayDate = new LocalDate().toDateTimeAtCurrentTime

        //Execute Features on Empty DataFrame
        val featExecutor = FeatExecutor(resolvedFeatures.featureSet, emptyBaseDF, todayDate)
        featExecutor.isLog = isLog
        val featureDF = featExecutor.execute()
        val featCols = featureDF.columns.toSet

        println("FEATURES CHAKKI")
        println(featCols.mkString(","))
        //Check Whether all Features are Requested.
        val expectedFeats = resolvedFeatures.reqFeatures ++ Set("customer_id")
        assert(featCols == expectedFeats, s"$execLevel generated and expected feature set doesn't match!!")
        println(s"Generated Feature set is as expected for $execLevel. Feature count: ${featCols.size}")

      } else {
        // Only Test Parsing
        val uniqTs = resolvedFeatures.featureSet.flatMap(_.timeAgg).distinct
        uniqTs.foreach { ts =>
          println(s"Parsing for timeSeries:$ts")
          val parsedFeatures = FeatParser(ts, resolvedFeatures.featureSet)
          if (isLog) parsedFeatures.logAll()
        }
      }
    }
  }

}
