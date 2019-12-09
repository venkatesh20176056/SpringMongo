package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.Evaluation.EvalResult._
import com.paytm.map.features.utils.DataFrameTestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class EvalResultTest() extends DataFrameTestBase {

  //  import spark.implicits._
  case class customerCat(customer_id: Long, top_cats: Seq[String]) // conventionally case class resides outside of methods..

  import spark.implicits._

  describe("generate data for model scoring - precision recall") {

    def twoDFCountRatio(ADF: DataFrame, BDF: DataFrame): Double = {

      val ratio = ADF.count().toDouble / BDF.count().toDouble
      ratio
    }

    val sizeUnit: Int = 50
    val positiveSampleRatio = 0.1

    it("should have proper map for model parameter from string") {

      val arginputStr = "concat_n:5,distanceThreshold:0.45,kendallThreshold:0.1"
      val actual = parseParamToMap(arginputStr)
      val expected = Map("concat_n" -> "5", "distanceThreshold" -> "0.45", "kendallThreshold" -> "0.1")
      assert(actual, expected)
      println(actual)

    }

    it("should (1-positiveSampleRatio) of seed = 50% of evalUniverse") {

      // common dataframe
      val positiveCategorySeq = (1 to 50).map(x => x.toString) // Seq[String] -> will be converted to sparse vector or used for exact match
      val negativeCategorySeq = (51 to 100).map(x => x.toString)
      val seedAllDF = spark.createDataFrame((1 to 2 * sizeUnit).map(x => customerCat(x.toLong, positiveCategorySeq))).toDF()
      val negativeUniverse = spark.createDataFrame((2 * sizeUnit + 1 to 4 * sizeUnit).map(x => customerCat(x.toLong, negativeCategorySeq))).toDF()
      val evalDFs = EvalSeedAndUniverseOffline.generate(spark, seedAllDF, negativeUniverse, 0.3)
      val seedAllCount = seedAllDF.count()
      val evalUniverse = evalDFs("universe")

      val acceptedMarginDiff = 0.2
      val ratioDiffAbs = math.abs(seedAllCount * (1 - 0.3) / evalUniverse.count() - 0.5)
      println(ratioDiffAbs, acceptedMarginDiff)
      assert(ratioDiffAbs < acceptedMarginDiff)

    }

    ignore("should have 50% of exact match result") {
      val positiveCategorySeq = (1 to 50).map(x => x.toString) // Seq[String] -> will be converted to sparse vector or used for exact match
      val negativeCategorySeq = (51 to 100).map(x => x.toString)
      val seedAllDF = spark.createDataFrame((1 to 2 * sizeUnit).map(x => customerCat(x.toLong, positiveCategorySeq))).toDF()
      val negativeUniverse = spark.createDataFrame((2 * sizeUnit + 1 to 4 * sizeUnit).map(x => customerCat(x.toLong, negativeCategorySeq))).toDF()
      val affinityDF = seedAllDF.union(negativeUniverse)
      val evalDFs = EvalSeedAndUniverseOffline.generate(spark, seedAllDF.select("customer_id"), negativeUniverse.select("customer_id"), positiveSampleRatio)
      val seedDF = evalDFs("seed")
      val evalUniverse = evalDFs("universe")

      evalUniverse.withColumn("source", when($"customer_id".lt(2 * sizeUnit + 1), "seed").otherwise("negative"))
        .groupBy("source").agg(count("customer_id")).show()
      val arginputStr = "concat_n:5,distanceThreshold:0.45,kendallThreshold:0.1"
      val exactParam = parseParamToMap(arginputStr)
      val exactResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "exact", exactParam, "max")
      val positiveFromExactResult = exactResult.where($"customer_id".lt(2 * sizeUnit + 1))
      val acceptedDiffAbs = 0.1
      val diffAbs = math.abs(0.5 - positiveFromExactResult.count().toDouble / evalUniverse.count().toDouble)
      val actual = diffAbs < acceptedDiffAbs

      val expected = true
      println(positiveFromExactResult.count(), exactResult.count(), diffAbs)
      assert(actual, expected)
      affinityDF.show

    }

  }

}

