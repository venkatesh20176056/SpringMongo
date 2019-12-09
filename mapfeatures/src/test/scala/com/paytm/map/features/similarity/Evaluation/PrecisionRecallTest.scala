package com.paytm.map.features.similarity.Evaluation

import com.paytm.map.features.similarity.Evaluation.EvalResult._
import com.paytm.map.features.similarity.Evaluation.PrecisionRecall._
import com.paytm.map.features.utils.DataFrameTestBase
import org.apache.spark.sql.functions.lit

case class customerCat(customer_id: Long, top_cats: Seq[String]) // conventionally case class resides outside of methods..

class PrecisionRecallTest() extends DataFrameTestBase {

  import spark.implicits._

  val sizeUnit: Int = 20
  val positiveSampleRatio = 0.5

  describe("precision recall generation") {

    // added global param setting
    val arginputStr = "concat_n:5,distanceThreshold:0.45,kendallThreshold:0.1"
    val randomParam = parseParamToMap(arginputStr)
    val LSHParam = parseParamToMap(arginputStr)
    val exactParam = parseParamToMap(arginputStr)

    it("should generated precision recall result > 0") {

      val positiveCategorySeq = (1 to 50).map(x => x.toString) // Seq[String] -> will be converted to sparse vector or used for exact match
      val partialPositiveCategorySeq = ((1 to 15) ++ util.Random.shuffle((16 to 20).toList).take(3)).map(x => x.toString)
      val negativeCategorySeq = (51 to 100).map(x => x.toString)
      val seedAllDF = spark.createDataFrame((1 to 2 * sizeUnit).map(x => customerCat(x.toLong, positiveCategorySeq))).toDF() /// 1-40
      val negativeUniverse = spark.createDataFrame((2 * sizeUnit + 1 to 4 * sizeUnit).map(x => customerCat(x.toLong, negativeCategorySeq))).toDF() // 41-80
      val fuzzyUniverse = spark.createDataFrame((4 * sizeUnit + 1 to 5 * sizeUnit).map(x => customerCat(x.toLong, partialPositiveCategorySeq))).toDF() // 81-100
      val affinityDF = seedAllDF.union(negativeUniverse).union(fuzzyUniverse).cache()

      // build static data frame instead of using evalUniverseBuild

      val seedAll = seedAllDF.select("customer_id")
      val negativeAll = negativeUniverse.select("customer_id")

      val seedUniverse = EvalSeedAndUniverseOffline.generate(spark, seedAll, negativeAll, positiveSampleRatio)
      val seedDF = seedUniverse("seed")
      val evalDF = seedUniverse("universe")
      val evalUniverse = evalDF
        .union(
          fuzzyUniverse.select("customer_id").limit(seedDF.count().toInt).withColumn("label", lit(1))
        ).cache()
      // put fuzzy customer 's label =1

      val exactResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "exact", exactParam, "max")
      val fuzzyResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "lsh", LSHParam, "max")
      val precisionRecallResult = precisionRecallCal(spark, exactResult, fuzzyResult, evalUniverse, 0.3)
      val actual = precisionRecallResult.count() > 0
      val expected = true

      assert(actual, expected)
      precisionRecallResult.show()

    }

    it("should have precision , recall at last K ") {

      val positiveCategorySeq = (1 to 50).map(x => x.toString) // Seq[String] -> will be converted to sparse vector or used for exact match
      val partialPositiveCategorySeq = ((1 to 15) ++ util.Random.shuffle((16 to 20).toList).take(3)).map(x => x.toString)
      val negativeCategorySeq = (51 to 100).map(x => x.toString)
      val seedAllDF = spark.createDataFrame((1 to 2 * sizeUnit).map(x => customerCat(x.toLong, positiveCategorySeq))).toDF()
      val negativeUniverse = spark.createDataFrame((2 * sizeUnit + 1 to 4 * sizeUnit).map(x => customerCat(x.toLong, negativeCategorySeq))).toDF()
      val fuzzyUniverse = spark.createDataFrame((4 * sizeUnit + 1 to 5 * sizeUnit).map(x => customerCat(x.toLong, partialPositiveCategorySeq))).toDF()
      val affinityDF = seedAllDF.union(negativeUniverse).union(fuzzyUniverse).cache()

      // build static data frame instead of using evalUniverseBuild

      val seedAll = seedAllDF.select("customer_id")
      val negativeAll = negativeUniverse.select("customer_id")
      val seedUniverse = EvalSeedAndUniverseOffline.generate(spark, seedAll, negativeAll, 0.6)

      val seedDF = seedUniverse("seed")
      val evalDF = seedUniverse("universe")

      val evalUniverse = evalDF
        .union(
          fuzzyUniverse.select("customer_id").limit(seedDF.count().toInt).withColumn("label", lit(1))
        ).cache()
      // put fuzzy customer 's label =1

      val exactResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "exact", exactParam, "max")
      val fuzzyResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "lsh", LSHParam, "max")
      val randomResult = modelResult(spark, seedDF, evalUniverse, Some(affinityDF), "random", randomParam, "max")
      val precisionRecallResult = precisionRecallCal(spark, exactResult, fuzzyResult, evalUniverse, 0.1)
      val actual = precisionRecallResult.orderBy($"k".desc).limit(1) // pick last K
        .where($"new_precision".gt(0.5))
        .where($"new_recall".gt(0.9))
        .where($"old_precision".gt(0.5))
        .where($"old_recall".gt(0.9)).count() > 0 // must have record which can meet these condition

      // final precision is supposed to be roughly 0.66 = 2/3 (fuzzy and positive captured out of all : fuzzy + positive + negative)

      val expected = true
      precisionRecallResult.orderBy($"k".desc).limit(1).show()
      assert(actual, expected)

    }

  }

} // end PrecisionRecallTest

