package com.paytm.map.features
import com.paytm.map.features.similarity.Model.{DefaultUser, OfflineUser, SimilarUserMethods}
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, max, size, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

package object similarity {

  val tagMap: Map[String, SimilarUserMethods] = Map(
    "boost_offline" -> OfflineUser,
    "default" -> DefaultUser
  )

  val num_partitions: Int = 2100
  val num_hashtables: Int = 3

  val logger: Logger = Logger.getLogger("LookalikeSimilarity")

  logger.setLevel(Level.INFO)

  val to_sparseUDF: UserDefinedFunction = udf[Vector, Seq[Int], Int](to_sparse)

  val computeKendallUDF: UserDefinedFunction = udf[Double, Seq[Int], Seq[Int]](computeKendall)

  val sliceUDF: UserDefinedFunction = udf((array: Seq[Int], from: Int, to: Int) => array.slice(from, to))

  val stringValUDF: UserDefinedFunction = udf((array: Seq[Int]) => array.mkString("_"))

  val maxCategoriesUDF: UserDefinedFunction = udf((value: Seq[Int]) => value.max)

  def baseDataPrep(colStrSeq: Seq[String], todayDate: DateTime, settings: Settings)(implicit spark: SparkSession): DataFrame = {

    val customerGroupSeq = 1 to 3 // remove group 4
    val basePath = s"${settings.featuresDfs.featuresTable}".replace("stg", "prod")

    val baseData = customerGroupSeq.map(x => {
      val latestDate = getLatestTableDate(s"$basePath$x/flatTable/", spark, todayDate.toString(ArgsUtils.formatter), "dt=")
      latestDate match {
        case Some(y) => {
          val df = spark.read.load(s"$basePath$x/flatTable/dt=$y")
            .select(colStrSeq.head, colStrSeq.tail: _*)
            .na.fill(0)
          df
        }
        case _ =>
          logger.error("Recent data string not found for flatTable")
          sys.exit(1)
      }
    }).reduce(_ union _).repartition(num_partitions)
    baseData
  }

  def to_sparse(arr: Seq[Int], max_categories: Int): Vector = {
    val indices = arr.toArray.sorted
    val len = indices.length
    val values = Array.fill(len) { 1.0 }
    new SparseVector(max_categories, indices, values)
  }

  def computeKendall(arrA: Seq[Int], arrB: Seq[Int]): Double = {
    val x: Array[Double] = arrA.map(_.toDouble).toArray
    val y: Array[Double] = arrB.map(_.toDouble).toArray
    val kc = new KendallsCorrelation
    kc.correlation(x, y)
  }

  /**
   * Create sparse representation for top n category.
   * @param df input dataFrame for transform
   * @return sparse feature dataFrame
   */
  def prepareSparseDF(df: DataFrame, concatN: Int): DataFrame = {
    import df.sparkSession.implicits._

    val tempDF = df.filter(size($"features") > 0)
      .withColumn("features", $"features".cast(ArrayType(IntegerType)))

    val maxCategories = tempDF
      .withColumn("max_value", maxCategoriesUDF($"features"))
      .agg(max($"max_value")).first()(0)

    val sparseDF = tempDF
      .withColumn("len_cats", size($"features"))
      .filter($"len_cats" >= concatN)
      .withColumn("features_n", sliceUDF($"features", lit(0), lit(concatN)))
      .withColumn("features_n_str", stringValUDF($"features_n"))
      .withColumn("features_sparse", to_sparseUDF($"features_n", lit(maxCategories) + 1))
      .drop("len_cats")
      .repartition(num_partitions, $"features_n_str")

    sparseDF
  }

  type OptionMap = Map[Symbol, String]

  def getOptionArgs(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = s(0) == '-'
    list match {
      case Nil => map
      case "--segment_id" :: value :: tail =>
        getOptionArgs(map ++ Map('segment_id -> value.toString), tail)
      case "--execution_timestamp" :: value :: tail =>
        getOptionArgs(map ++ Map('execution_timestamp -> value.toString), tail)
      case "--tag" :: value :: tail =>
        getOptionArgs(map ++ Map('tag -> value.toString), tail)
      case "--user_limit" :: value :: tail =>
        getOptionArgs(map ++ Map('user_limit -> value.toString), tail)
      case "--user_base_path" :: value :: tail =>
        getOptionArgs(map ++ Map('user_base_path -> value.toString), tail)

      case "--concat_n" :: value :: tail =>
        getOptionArgs(map ++ Map('concat_n -> value.toString), tail)
      case "--distance_threshold" :: value :: tail =>
        getOptionArgs(map ++ Map('distance_threshold -> value.toString), tail)
      case "--kendall_threshold" :: value :: tail =>
        getOptionArgs(map ++ Map('kendall_threshold -> value.toString), tail)

      case "--seed_path" :: value :: tail =>
        getOptionArgs(map ++ Map('seed_path -> value.toString), tail)
      case "--positive_sample_ratio" :: value :: tail =>
        getOptionArgs(map ++ Map('positive_sample_ratio -> value.toString), tail)
      case "--ground_truth_universe_path" :: value :: tail =>
        getOptionArgs(map ++ Map('ground_truth_universe_path -> value.toString), tail)

      case "--new_model_result_path" :: value :: tail =>
        getOptionArgs(map ++ Map('new_model_result_path -> value.toString), tail)
      case "--old_model_result_path" :: value :: tail =>
        getOptionArgs(map ++ Map('old_model_result_path -> value.toString), tail)
      case "--negative_universe_path" :: value :: tail =>
        getOptionArgs(map ++ Map('negative_universe_path -> value.toString), tail)
      case "--precision_recall_result_path" :: value :: tail =>
        getOptionArgs(map ++ Map('precision_recall_result_path -> value.toString), tail)

      case "--old_model_name" :: value :: tail =>
        getOptionArgs(map ++ Map('old_model_name -> value.toString), tail)
      case "--new_model_name" :: value :: tail =>
        getOptionArgs(map ++ Map('new_model_name -> value.toString), tail)

      case "--old_model_param" :: value :: tail =>
        getOptionArgs(map ++ Map('old_model_param -> value.toString), tail)
      case "--new_model_param" :: value :: tail =>
        getOptionArgs(map ++ Map('new_model_param -> value.toString), tail)

      case "--old_model_vector" :: value :: tail =>
        getOptionArgs(map ++ Map('old_model_vector -> value.toString), tail)
      case "--new_model_vector" :: value :: tail =>
        getOptionArgs(map ++ Map('new_model_vector -> value.toString), tail)

      case "--eval_seed_universe_type" :: value :: tail =>
        getOptionArgs(map ++ Map('eval_seed_universe_type -> value.toString), tail)

      case "--rm_customers_only_clicks" :: value :: tail =>
        getOptionArgs(map ++ Map('rm_customers_only_clicks -> value.toString), tail)
      case "--param_combination_size" :: value :: tail =>
        getOptionArgs(map ++ Map('param_combination_size -> value.toString), tail)
      case "--universe_path" :: value :: tail =>
        getOptionArgs(map ++ Map('universe_path -> value.toString), tail)
      case "--pooling" :: value :: tail =>
        getOptionArgs(map ++ Map('pooling -> value.toString), tail)

      case string :: opt2 :: tail if isSwitch(opt2) =>
        getOptionArgs(map ++ Map('user_base_path -> string), list.tail)
      case string :: Nil => getOptionArgs(map ++ Map('user_base_path -> string), list.tail)
      case option :: tail =>
        println(option)
        sys.exit(1)
    }
  }
}
