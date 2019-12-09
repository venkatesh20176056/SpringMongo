package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.config.Schemas.SchemaRepo
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.UDFs.readTableV3
import org.apache.spark.sql.types.LongType
import org.joda.time.DateTime

import com.paytm.map.features.base.DataTables.DFCommons

object MedicalFeatures extends MedicalFeaturesJob
    with SparkJob with SparkJobBootstrap {

}

trait MedicalFeaturesJob {
  this: SparkJob =>

  import org.apache.spark.sql.functions._

  val JobName = "MedicalFeatures"

  val healthCategoryCol = "health_category"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  val healthCategories = Seq("Pharmacy", "Wellness", "Doctors and Clinic", "Hospital", "Pathology Lab")

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark

    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val lookBackDays: Int = args(1).toInt
    val dtSeq = BaseTableUtils.dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    val outPath = s"${settings.featuresDfs.baseDFS.aggPath}/MedicalFeaturesCustomer"
    val anchorPath = s"${settings.featuresDfs.baseDFS.aggPath}/MedicalFeaturesCustomerAnchor"

    val healthTransactions: DataFrame = getHealthTransactions(spark, settings, targetDateStr, lookBackDays, dtSeq)

    val totalCount = count(lit(1)).as("transaction_count")
    val totalSum = sum("txn_amount").as("transaction_amount")
    val lastTransaction = max("dt").as("last_transaction")
    val mainAggs = Seq(totalCount, totalSum, lastTransaction)

    val categoryAggs = healthCategories.flatMap(cat => Seq(catCountColumn(cat), catAmountColumn(cat)))

    val aggs = mainAggs ++ categoryAggs

    val medicalFeatures =
      healthTransactions
        .groupBy("customer_id", "dt")
        .agg(aggs.head, aggs.tail: _*)
        .addPrefixToColumns("MEDICAL_", Seq("customer_id", "dt"))

    val dataDf = {
      medicalFeatures
        .alignSchema(SchemaRepo.MedicalUserFeaturesSchema)
        .where(col("dt").between(dtSeq.head, dtSeq.last))
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDf.moveHDFSData(dtSeq, outPath)
  }

  def catCountColumn(cat: String): Column = {
    val name = s"${label(cat)}_count"
    val baseCol = when(col(healthCategoryCol) === cat, lit(1)).otherwise(lit(0))
    sum(baseCol).as(name)
  }

  def catAmountColumn(cat: String): Column = {
    val name = s"${label(cat)}_amount"
    val baseCol = when(col(healthCategoryCol) === cat, col("txn_amount")).otherwise(lit(0))
    sum(baseCol).as(name)
  }

  def label(cat: String): String = {
    cat.replaceAll(" ", "_")
  }

  def getHealthTransactions(spark: SparkSession, settings: Settings, targetDateStr: String, lookbackDays: Int, dtSeq: Seq[String]): DataFrame = {
    import spark.sqlContext.implicits._

    val offOrgMerch = readTableV3(spark, settings.datalakeDfs.offOrgMerchant)

    val offlineRetailer =
      readTableV3(spark, settings.datalakeDfs.offRetailer)
        .select(
          $"intcust_id".cast(LongType).as("cust_id"),
          $"mobile"
        )

    val merchant = readTableV3(spark, settings.datalakeDfs.merchantUser)

    val str =
      spark.read.parquet(settings.featuresDfs.baseDFS.strPath)
        .where($"txn_status" === 1)
        .where(to_date($"dt") > date_add(lit(targetDateStr), -lookbackDays))
        .withColumn("dt", to_date($"dt"))

    val healthCategories = Seq("healthcare", "beauty", "wellness")
    val excludedSubCategories = Seq("spa", "dietician", "salon and parlour")

    val healthOffline =
      offOrgMerch
        .where(lower(col("pg_category")).isin(healthCategories: _*))
        .where(!lower(col("pg_sub_category")).isin(excludedSubCategories: _*))
        .where(lower(col("Agent_Type")) === "key accounts")
        .withColumn(healthCategoryCol, categoryRename)
        .withColumn("mid", offOrgMerch("pg_MID"))
        .join(merchant, offOrgMerch("pg_MID") === merchant("name"))
        .cache()

    val p2mTransactions =
      healthOffline.join(str.where(str("txn_type") === 1), healthOffline("id") === str("payee_id"))
        .select(
          "customer_id",
          "mid",
          healthCategoryCol,
          "txn_amount",
          "dt"
        )

    val p2pTransactions =
      healthOffline
        .join(offlineRetailer, $"contact_phone_no" === $"mobile")
        .join(str.where(str("txn_type") === 69), offlineRetailer("cust_id") === str("payee_id"))
        .select(
          "customer_id",
          "mid",
          healthCategoryCol,
          "txn_amount",
          "dt"
        )
    val transactions = p2mTransactions.union(p2pTransactions).cache()

    val outPath = s"${settings.featuresDfs.baseDFS.aggPath}/MedicalTransactions"
    val anchorPath = s"${settings.featuresDfs.baseDFS.aggPath}/MedicalTransactionsAnchor"

    val dataDf = {
      transactions
        .alignSchema(SchemaRepo.MedicalTransactionSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDf.moveHDFSData(dtSeq, outPath)

    dataDf
  }

  def categoryRename: Column = {
    when(lower(col("pg_sub_category")) === "pharmacy", "Pharmacy")
      .when(lower(col("pg_sub_category")) === "gym and fitness", "Wellness")
      .when(lower(col("pg_sub_category")).isin("doctors and clinic", "homeopathic clinic", "physiotherapy clinic"), "Doctors and Clinic")
      .when(lower(col("pg_sub_category")) === "hospital", "Hospital")
      .when(lower(col("pg_sub_category")) === "pathology lab", "Pathology Lab")
  }

}

