package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.UDFs.readTableV3
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.config.Schemas.SchemaRepo
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object InsuranceFeatures extends InsuranceFeaturesJob
  with SparkJob with SparkJobBootstrap

trait InsuranceFeaturesJob {
  this: SparkJob =>

  val JobName = "InsuranceFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark
    import spark.sqlContext.implicits._
    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val startDate: DateTime = targetDate.minusDays(lookBackDays)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = startDate.toString(ArgsUtils.formatter)
    val dtSeq = BaseTableUtils.dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Output path
    val anchorPath = s"${settings.featuresDfs.baseDFS.anchorPath}/Insurance"
    val outPath = s"${settings.featuresDfs.baseDFS.aggPath}/InsuranceFeatures"

    val insuranceBase = getInsuranceBase(settings, targetDateStr, startDateStr)

    val categoryTxnCols = Seq("category_id", "category_name", "created_at", "product_id", "size").map(col(_))

    val insuranceAggs = {
      insuranceBase
        .withColumnRenamed("selling_price", "size")
        .withColumn("category_txn_count", struct(col("category_id"), lit(1) as "count"))
        .withColumn("rank", row_number().over(Window.partitionBy("customer_id", "dt", "category_id").orderBy(desc("created_at"))))
        .withColumn("category_txn", struct(categoryTxnCols: _*))
        .groupBy("customer_id", "dt")
        .agg(
          collect_set(col("category_txn_count")) as "category_txn_count",
          collect_set(when($"rank" === 1, $"category_txn")) as "last_category_txn"
        )
        .addPrefixToColumns("INSURANCE_", Seq("customer_id", "dt"))
        .alignSchema(SchemaRepo.InsuranceSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    insuranceAggs.moveHDFSData(dtSeq, outPath)

  }

  def getInsuranceBase(settings: Settings, targetDateStr: String, startDateStr: String)(implicit spark: SparkSession): DataFrame = {

    import spark.sqlContext.implicits._

    val sales =
      spark.read.parquet(settings.featuresDfs.baseDFS.salesDataPath)
        .where($"dt".between(startDateStr, targetDateStr))
        .where(!($"status".isin(6, 8)) && $"vertical_id".isin(79, 95))
        .where($"payment_status" === 2)

    val metaSchema = StructType(Seq(
      StructField("cust_id", StringType, nullable = true),
      StructField("premium", DoubleType, nullable = true)
    ))

    val so_meta =
      readTableV3(spark, settings.datalakeDfs.salesOrderMetadata, targetDateStr, startDateStr)
        .withColumn("json_data", from_json($"meta_data", metaSchema))
        .withColumn("premium", $"json_data"("premium"))
        .withColumn("cust_id", $"json_data"("cust_id"))

    val catalog_prod =
      DataTables.catalogProductTable(spark, settings.datalakeDfs.catalog_product)
        .where($"vertical_id".isin(79, 95))

    val catalog_cat = broadcast(DataTables.catalogCategoryTable(spark, settings.datalakeDfs.catalog_category))

    val joined =
      sales
        .join(so_meta, Seq("order_item_id"), "left")
        .join(catalog_prod, sales("product_id") === catalog_prod("product_id"), "left")
        .join(catalog_cat, catalog_prod("category_id") === catalog_cat("category_id"))

    joined.select(
      catalog_prod("category_id") as "category_id",
      catalog_cat("name") as "category_name",
      sales("product_id") as "product_id",
      coalesce(so_meta("cust_id"), sales("customer_id")) as "customer_id",
      coalesce(so_meta("premium"), sales("selling_price") / 1000) as "selling_price",
      to_date(sales("created_at")) as "dt",
      sales("created_at") as "created_at"
    )
  }

  /*
select
  d.category_id,e.name as category_name,coalesce(c.cust_id,b.customer_id) as customer_id,coalesce(c.premium,a.selling_price/1000) as selling_price
from
  (select *
  from
    marketplace.sales_order_item_snapshot_v3 where status not in (6,8) and vertical_id in (79,95)
   ) a
  join
    (select * from marketplace.sales_order_snapshot_v3 where payment_status=2) b
        on a.order_id=b.id
  left join
  (select *,get_json_object(meta_data,'$.cust_id') as cust_id,get_json_object(meta_data,'$.premium') as premium
      from marketplace.sales_order_metadata_snapshot_v3) c
      on a.id=c.order_item_id
  left join (select * from marketplace.catalog_product_snapshot_v3 where vertical_id in (79,95)) d
      on a.product_id=d.id
  left join marketplace_catalog.catalog_category_snapshot_v3 e
      on d.category_id=e.id
 */

}

