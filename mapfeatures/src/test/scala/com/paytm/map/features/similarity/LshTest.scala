package com.paytm.map.features.similarity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.paytm.map.features.similarity.Model.LSHModel
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{ArrayType, IntegerType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FunSuite, Matchers, Tag}

class LshTest extends FunSuite with DataFrameSuiteBase with Matchers {

  test("Sparse data creation test", Tag("SparseTest")) {
    import spark.implicits._

    val inputDF: DataFrame = Seq(
      (1, Seq("1", "3", "5", "2", "8")),
      (2, Seq("20", "50", "1", "10", "11"))
    )
      .toDF("customer_id", "features")

    val expectedDF = Seq(
      (1, Vectors.sparse(51, Seq((1, 1.0), (2, 1.0), (3, 1.0), (5, 1.0), (8, 1.0)))),
      (2, Vectors.sparse(51, Seq((1, 1.0), (10, 1.0), (11, 1.0), (20, 1.0), (50, 1.0))))
    ).toDF("customer_id", "features_sparse")

    val actualDF = prepareSparseDF(inputDF, 5)
      .select("customer_id", "features_sparse")

    assertDataFrameEquals(
      expectedDF.orderBy("customer_id"),
      actualDF.orderBy("customer_id")
    )

  }

  test("Sparse data creation with < 'concat_n' size top_cats", Tag("SparseTest")) {

    import spark.implicits._

    val schema = StructType(
      StructField("customer_id", IntegerType, nullable = false) ::
        StructField("features", ArrayType(IntegerType), nullable = true) :: Nil
    )

    val inputDF = Seq(
      (1, Seq("1", "3", "5")),
      (2, Seq("20", "50"))
    ).toDF("customer_id", "features")

    val expectedDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val actualDF = prepareSparseDF(inputDF, 5)
      .select("customer_id", "features")

    assertDataFrameEquals(expectedDF, actualDF)

  }

  test("Sparse data creation with empty top_cats", Tag("SparseTest")) {
    implicit val sparkSession: SparkSession = spark
    import sparkSession.implicits._

    val inputDF = Seq(
      (1, Seq()),
      (2, Seq())
    ).toDF("customer_id", "features")

    val schema = StructType(
      StructField("customer_id", IntegerType, nullable = false) ::
        StructField("features", ArrayType(IntegerType), nullable = true) :: Nil
    )

    val expectedDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val actualDF = prepareSparseDF(inputDF, 5)
      .select("customer_id", "features")

    assertDataFrameEquals(expectedDF, actualDF)

  }

  test("Build LSH Model Test", Tag("LSH")) {
    import spark.implicits._

    val inputDF = Seq(
      (1, Vectors.sparse(51, Seq((1, 1.0), (2, 1.0), (3, 1.0), (5, 1.0), (8, 1.0)))),
      (2, Vectors.sparse(51, Seq((1, 1.0), (10, 1.0), (11, 1.0), (20, 1.0), (50, 1.0))))
    ).toDF("customer_id", "features_sparse")

    val expectedSchema = StructType(
      StructField("customer_id", IntegerType, nullable = false) ::
        StructField("features_sparse", VectorType, nullable = true) ::
        StructField("hashes", ArrayType(VectorType), nullable = true) :: Nil
    )

    val modelInfo = new LSHModel().buildModel(inputDF)

    assert(expectedSchema, modelInfo.df.schema)

  }

  test("Compute LSH test", Tag("LSH")) {
    import spark.implicits._

    val seedDF = spark.createDataFrame(Seq(
      (0, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(0, 1, 2))),
      (1, Seq(3, 5), "3_5", Vectors.sparse(7, Seq((3, 1.0), (5, 1.0))), Seq(Vectors.dense(3, 5))),
      (2, Seq(0, 2, 4), "0_2_4", Vectors.sparse(7, Seq((0, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(0, 2, 4)))
    )).toDF("customer_id", "features_n", "features_n_str", "features_sparse", "hashes")

    val universeDF = spark.createDataFrame(Seq(
      (3, Seq(1, 2, 4), "1_2_4", Vectors.sparse(7, Seq((1, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(1, 2, 4))),
      (4, Seq(0, 2, 4), "0_2_4", Vectors.sparse(7, Seq((0, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(0, 2, 4))),
      (5, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(0, 1, 2))),
      (6, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(0, 1, 2)))
    )).toDF("customer_id", "features_n", "features_n_str", "features_sparse", "hashes")

    val schema = StructType(
      StructField("seed_cats", ArrayType(IntegerType, false), nullable = true) ::
        StructField("neighbor_cats", ArrayType(IntegerType, false), nullable = true) ::
        StructField("JaccardDistance", DoubleType, nullable = true) :: Nil
    )

    val expectedData = Seq(
      Row(Seq(0, 1, 2), Seq(0, 1, 2), 0.0),
      Row(Seq(0, 2, 4), Seq(0, 2, 4), 0.0)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData), schema
    )

    val actualDF = new LSHModel()
      .computeSimilarUsers(
        "src/test/resources/model",
        seedDF, universeDF, 0.45
      )
      .select("seed_cats", "neighbor_cats", "JaccardDistance")
      .filter($"JaccardDistance" === 0)

    assertDataFrameEquals(
      expectedDF.orderBy($"seed_cats"),
      actualDF.orderBy($"seed_cats")
    )

  }

  test("Compute LSH with no neighbors", Tag("LSH")) {
    import spark.implicits._

    val seedDF = spark.createDataFrame(Seq(
      (0, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(0.55, 1, 2))),
      (1, Seq(3, 5), "3_5", Vectors.sparse(7, Seq((3, 1.0), (5, 1.0))), Seq(Vectors.dense(0.09, 5))),
      (2, Seq(0, 2, 4), "0_2_4", Vectors.sparse(7, Seq((0, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(9, 2, 4)))
    )).toDF("customer_id", "features_n", "features_n_str", "features_sparse", "hashes")

    val universeDF = spark.createDataFrame(Seq(
      (3, Seq(1, 2, 4), "1_2_4", Vectors.sparse(7, Seq((1, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(10.5, 2, 4))),
      (4, Seq(0, 2, 4), "0_2_4", Vectors.sparse(7, Seq((0, 1.0), (2, 1.0), (4, 1.0))), Seq(Vectors.dense(0.0001, 2, 4))),
      (5, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(0.0001, 2, 4))),
      (6, Seq(0, 1, 2), "0_1_2", Vectors.sparse(7, Seq((0, 1.0), (1, 1.0), (2, 1.0))), Seq(Vectors.dense(11.99, 1, 2)))
    )).toDF("customer_id", "features_n", "features_n_str", "features_sparse", "hashes")

    val actualDF = new LSHModel()
      .computeSimilarUsers(
        "src/test/resources/model",
        seedDF, universeDF, 0.4
      )
      .select("seed_cats", "neighbor_cats", "JaccardDistance")
      .filter($"JaccardDistance" === 0)

    val expectedCount = 0

    assert(expectedCount, actualDF.count)

  }

  test("Compute Kendall Test", Tag("LSH")) {
    implicit val sparkSession: SparkSession = spark
    import spark.implicits._

    val inputDF = Seq(
      (1, Seq(1, 3, 5), Seq(1, 4, 5)),
      (2, Seq(2, 1, 4), Seq(4, 5, 2))
    ).toDF("customer_id", "seed_cats", "neighbor_cats")

    val actualDF = new LSHModel().computeKendallScore(inputDF, 0.1)

    val expectedDF = Seq(
      (1, 1.0)
    ).toDF("customer_id", "KendallTau")

    assertDataFrameEquals(expectedDF, actualDF)

  }

}
