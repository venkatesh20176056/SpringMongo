package com.paytm.map.features.base

import java.net.URI

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.Constants._
import com.paytm.map.features.utils.ArgsUtils.formatter
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

object DFCache extends DFCacheJob
  with SparkJob with SparkJobBootstrap

trait DFCacheJob {
  this: SparkJob =>

  val JobName = "DFCache"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val jobID: String = args(2).toString
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = targetDate.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val startDateProdSpan = targetDate.minusDays(productSpanOffset).toString(ArgsUtils.formatter)

    val lookBackGAOffset: Int = gaDelayOffset // GA data is available at 2 days
    val targetDateGA = targetDate.minusDays(lookBackGAOffset)
    val startDateGA = targetDateGA.minusDays(gaBackFillDays)

    // Parse Path Config
    val baseDFS = settings.featuresDfs.baseDFS

    // Make the DataFrame
    // DataTables.soiTable now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    lazy val soi = DataTables.soiTable(spark, settings.datalakeDfs.salesOrderItem, targetDateStr, startDateStr)
    lazy val so = DataTables.soTable(spark, settings.datalakeDfs.salesOrder, targetDateStr, startDateStr)
    lazy val sop = DataTables.sopTable(spark, settings.datalakeDfs.salesOrderPayment, targetDateStr, startDateStr)
    lazy val promocodeTable = DataTables.cashbackPromoUsagesTable(spark, settings.datalakeDfs.promoCodeUsage, targetDateStr, startDateStr)
    lazy val catalogProductTable = DataTables.catalogProductTable(spark, settings.datalakeDfs.catalog_product)
    lazy val metaData = DataTables.metaDataTable(spark, settings.datalakeDfs.salesOrderMetadata, targetDateStr, startDateStr)
    lazy val rechargeTable = DataTables.rechargeTable(spark, settings.datalakeDfs.catalogVerticalRecharge)
    // DataTables.newSTRTable now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    lazy val newSTRTable = DataTables.newSTRTable(spark, settings.datalakeDfs.newSystemTxnRequest, targetDateStr, startDateStr)
    lazy val merchantTable = DataTables.merchantTable(spark, settings.datalakeDfs.merchantUser)
    lazy val catalogCategoryGLPTable = DataTables.catalogCategoryGLPTable(spark, settings.datalakeDfs.catalog_category)
    lazy val catalogEntityDecoratorTable = DataTables.catalogEntityDecoratorTable(spark, settings.datalakeDfs.catalog_entity_decorator)

    lazy val lmsAccount = DataTables.lmsAccount(spark, settings.datalakeDfs.lmsAccount)
    lazy val lmsContactability = DataTables.lmsContactability(spark, settings.datalakeDfs.lmsContactability)
    lazy val lmsAccountV2 = DataTables.lmsAccountV2(spark, settings.datalakeDfs.lmsAccountV2)
    lazy val lmsCasesV2 = DataTables.lmsCasesV2(spark, settings.datalakeDfs.lmsCaseV2)
    lazy val lmsContactabilityV2 = DataTables.lmsContactabilityV2(spark, settings.datalakeDfs.lmsContactabilityV2)

    lazy val accountMaster = DataTables.TBAADMGeneralAcctMast(spark, settings.datalakeDfs.TBAADMGeneralAcctMast)
    lazy val histTxnTbl = DataTables.TBAADMHistTranDtl(spark, settings.datalakeDfs.TBAADMHistTranDtl, targetDateStr, startDateStr)

    /////////////////////////// Hard Cache the Tables  ///////////////////////////////
    // This table now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    def salesTable(): Unit = {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      so.join(soi, "order_id")
        .where($"dt".between(startDateStr, targetDateStr))
        .repartitionByRange((lookBackDays + 1) * 10, $"dt", $"customer_id")
        .write.mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(baseDFS.salesDataPath)
    }

    // This table now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    def salesPromoTable(): Unit = {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      spark.read.parquet(baseDFS.salesDataPath)
        .where($"dt".between(startDateStr, targetDateStr))
        .join(promocodeTable.drop("status").drop("dt"), Seq("order_item_id", "customer_id"), "left_outer")
        .repartitionByRange((lookBackDays + 1) * 10, $"dt", $"customer_id")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(baseDFS.salesPromoPath)
    }

    // This table now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    def salesPromoCategoryTable(): Unit = {
      // as a optimization step, divide the actual join in two parts, skewed value broadcast join & rest simple join.
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      spark.read.parquet(baseDFS.salesPromoPath)
        .where($"dt".between(startDateStr, targetDateStr))
        .skewJoin(catalogProductTable.drop("vertical_id"), "product_id", Seq("product_id"), "left_outer")
        .repartitionByRange((lookBackDays + 1) * 10, $"dt", $"customer_id")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(baseDFS.salesPromoCategoryPath)
    }

    def salesPromoMetaTable(): Unit = {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      spark.read.parquet(baseDFS.salesPromoPath)
        .where($"dt".between(startDateStr, targetDateStr))
        .where(col("successfulTxnFlag") === 1)
        .join(metaData, Seq("order_item_id"), "left_outer")
        .join(sop, Seq("order_id"), "left_outer")
        .repartitionByRange((lookBackDays + 1) * 10, $"dt", $"customer_id")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(baseDFS.salesPromMetaPath)
    }

    // This table now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    def salesPromoRechTable(): Unit = {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      spark.read.parquet(baseDFS.salesPromoPath)
        .where($"dt".between(startDateStr, targetDateStr))
        .skewJoin(rechargeTable, "product_id", Seq("product_id"), "left_outer")
        .repartitionByRange((lookBackDays + 1) * 10, $"dt", $"customer_id")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(baseDFS.salesPromRechPath)
    }

    def strTable(): Unit = {
      newSTRTable.where(col("successfulTxnFlag") === 1).write.mode(SaveMode.Overwrite).parquet(baseDFS.strPath)
    }

    // This table now contains unfiltered data(for the last order feature) i.e. successful as well as unsuccessful transactions.For getting ONLY successful transactions data please add(from the place you are using this) the filter .where(col("successfulTxnFlag") === 1)
    def strMerchantTable(): Unit = {
      val merchantTableColDropped = merchantTable
        .drop("merchant_id")
        .drop("mid")

      newSTRTable
        .drop("alipay_trans_id")
        .join(broadcast(merchantTableColDropped), Seq("wallet_id"), "inner")
        .repartition(500, col("customer_id"))
        .write.mode(SaveMode.Overwrite).parquet(baseDFS.strMerchantPath)
    }

    def gaAggregatedTable(): Unit = {
      import spark.implicits._

      val gaAppWebTable = DataTables.gaJoinedWebApp(
        spark,
        settings.datalakeDfs.gaAppPaths,
        settings.datalakeDfs.gaWebPaths,
        settings.datalakeDfs.gaMallAppPaths,
        settings.datalakeDfs.gaMallWebPaths,
        targetDateGA, startDateGA
      )
        .persist(StorageLevel.MEMORY_AND_DISK)

      val catalogProductTableGA = catalogProductTable.select($"product_id", $"category_id", $"vertical_id").distinct().cache()

      val rechargeTableGA = rechargeTable.select($"product_id", $"service").distinct().cache()

      // Joining ga data with catalog product to get category_id, vertical_id (for EC, BK levels)
      // and further joined with recharge table to get "service" type (for RU levels)
      // concept of session is introduced, session is seen as activity of duration 30 mins on same page (product_id)
      val gaJoinedProduct = gaAppWebTable
        .join(catalogProductTableGA, Seq("product_id"), "left_outer")

      val gaJoinedProductRcharge = gaJoinedProduct.join(rechargeTableGA, Seq("product_id"), "left_outer")

      // Getting aggregated views and impressions, grouped for each category_id
      // creating a session identifier to treat any visit impression on the same  within 30 mins
      val gaAggregated = gaJoinedProductRcharge
        .groupBy(
          "customer_id",
          "dt",
          "category_id",
          "vertical_id",
          "service"
        )
        .agg(
          countDistinct(when(($"isApp" === 1 && $"event_action" === "Product Impression"), $"session_identifier").otherwise(null)).as("product_views_app"),
          countDistinct(when(($"isApp" === 1 && $"event_action" === "Product Click"), $"session_identifier").otherwise(null)).as("product_clicks_app"),
          countDistinct(when(($"isApp" === 0 && $"event_action" === "Product Impression"), $"session_identifier").otherwise(null)).as("product_views_web"),
          countDistinct(when(($"isApp" === 0 && $"event_action" === "Product Click"), $"session_identifier").otherwise(null)).as("product_clicks_web"),
          countDistinct(when(($"isApp" === 1 && (($"event_action" === "Product Detail") || ($"hit_type" === "APPVIEW" && $"app_current_name".startsWith("/p/")))), $"session_identifier").otherwise(null)).as("pdp_sessions_app"),
          countDistinct(when(($"isApp" === 0 && (($"event_action" === "Product Detail") || ($"hit_type" === "APPVIEW" && $"app_current_name".startsWith("/p/")))), $"session_identifier").otherwise(null)).as("pdp_sessions_web"),
          countDistinct(when($"isApp" === 0 && $"isMall" === 0, $"session_identifier").otherwise(null)).as("paytm_web"),
          countDistinct(when($"isApp" === 0 && $"isMall" === 1, $"session_identifier").otherwise(null)).as("mall_web"),
          countDistinct(when($"isApp" === 1 && $"isMall" === 0, $"session_identifier").otherwise(null)).as("paytm_app"),
          countDistinct(when($"isApp" === 1 && $"isMall" === 1, $"session_identifier").otherwise(null)).as("mall_app")
        )

      val gaAggregatePath = baseDFS.gaAggregatePath(targetDateGA)
      gaAggregated.write.mode(SaveMode.Overwrite).parquet(gaAggregatePath)

    }

    def lmsTable(): Unit = {
      lmsAccount
        .join(lmsContactability, Seq("customer_id"), "left_outer")
        .write.mode(SaveMode.Overwrite)
        .parquet(baseDFS.lmsTablePath)

      DataTables.lmsAccountV2(spark, lmsAccountV2, lmsCasesV2)
        .join(lmsContactabilityV2, Seq("user_id"), "left_outer")
        .withColumnRenamed("user_id", "customer_id")
        .write.mode(SaveMode.Overwrite)
        .parquet(baseDFS.lmsTablePathV2)
    }

    def gaAggregatedTableLP(): Unit = {
      import spark.implicits._

      // This req_cats file should be present in resources/required_categories folder for prod and stg
      // A copy can be found at azkaban/resources/req_cats
      val requiredECCatPath = settings.featuresDfs.resources + "required_categories_ec/req_cats"
      val reqCategoriesEC = spark.read.text(requiredECCatPath).collect.map(r => r.getString(0)).map(_.toInt)

      val gaAppWebRawTable = DataTables.gaJoinedWebAppRaw(
        spark,
        settings.datalakeDfs.gaAppPaths,
        settings.datalakeDfs.gaWebPaths,
        settings.datalakeDfs.gaMallAppPaths,
        settings.datalakeDfs.gaMallWebPaths,
        targetDateGA, startDateGA
      )
        .select($"app_current_name", $"customer_id", $"dt", $"session_identifier", $"isApp")
        .filter(($"app_current_name".contains("glpid-")) or $"app_current_name".contains("clpid-"))
        .persist(StorageLevel.MEMORY_AND_DISK)

      gaAppWebRawTable.count()

      // As there are many orphan CLPs with placeholder categories
      // we simply do join on clpIDs and glpIDs which are required from EC point of view
      val reqCLPIds = catalogEntityDecoratorTable.filter($"category_id".isin(reqCategoriesEC: _*))
        .select($"clpID").map(row => row.getAs[Int](0)).collect()

      val reqGLPIds = catalogCategoryGLPTable.filter($"category_id".isin(reqCategoriesEC: _*))
        .select($"glpID").map(row => row.getAs[Int](0)).collect()

      val gaAppWebRawTableCLP = gaAppWebRawTable
        .withColumn(
          "clpID",
          when(
            $"app_current_name".contains("clpid-"),
            split(
              regexp_extract($"app_current_name", "(clpid-\\d{2,8})", 1),
              "-"
            )(1).cast(IntegerType)
          ).otherwise(lit(null))
        )
        .filter($"clpID".isNotNull)
        .filter($"clpID".isin(reqCLPIds: _*))

      val gaAppWebRawTableGLP = gaAppWebRawTable
        .withColumn(
          "glpID",
          when(
            $"app_current_name".contains("glpid-"),
            split(
              regexp_extract($"app_current_name", "(glpid-\\d{2,8})", 1),
              "-"
            )(1).cast(IntegerType)
          ).otherwise(lit(null))
        )
        .filter($"glpID".isNotNull)
        .filter($"glpID".isin(reqGLPIds: _*))

      val catalogEntityCLP = catalogEntityDecoratorTable.select($"clpID", $"category_id").distinct()
      val catalogCategoryGLP = catalogCategoryGLPTable.select($"glpID", $"category_id").distinct()

      // Joining raw ga data with corresponding GLP and CLP tables to get corresponding category_ids
      // concept of session is introduced, session is seen as activity of duration 30 mins on same page (product_id)
      val gaJoinedCLP = gaAppWebRawTableCLP
        .join(catalogEntityCLP, Seq("clpID"), "left_outer")

      val gaJoinedGLP = gaAppWebRawTableGLP
        .join(catalogCategoryGLP, Seq("glpID"), "left_outer")

      // Getting aggregated sessions, grouped for each category_id
      // creating a session identifier to treat any visit impression on the same  within 30 mins
      val gaAggregatedCLP = gaJoinedCLP
        .groupBy(
          "customer_id",
          "dt",
          "category_id"
        )
        .agg(
          countDistinct(when(($"isApp" === 1), $"session_identifier").otherwise(null)).as("clp_sessions_app"),
          countDistinct(when(($"isApp" === 0), $"session_identifier").otherwise(null)).as("clp_sessions_web")
        )

      val gaAggregatedGLP = gaJoinedGLP
        .groupBy(
          "customer_id",
          "dt",
          "category_id"
        )
        .agg(
          countDistinct(when(($"isApp" === 1), $"session_identifier").otherwise(null)).as("glp_sessions_app"),
          countDistinct(when(($"isApp" === 0), $"session_identifier").otherwise(null)).as("glp_sessions_web")
        )

      val gaAggregated = gaAggregatedCLP.join(gaAggregatedGLP, Seq("customer_id", "dt", "category_id"), "outer")

      val gaAggregateLPPath = baseDFS.gaAggregateLPPath(targetDateGA)
      gaAggregated.write.mode(SaveMode.Overwrite).parquet(gaAggregateLPPath)
    }

    def deviceSignalTable() = {
      val startDate = formatter.parseDateTime(startDateStr)
      val minDate = formatter.parseDateTime("2016-01-01")
      val days = BaseTableUtils.dayIterator(startDate, targetDate, ArgsUtils.formatter)
      val fs = FileSystem.get(new URI(s"${baseDFS.deviceSignalPath}"), spark.sparkContext.hadoopConfiguration)

      // Delete everything older than startDate
      val daysBeforeStartDate = BaseTableUtils.dayIterator(minDate, startDate.minusDays(1), ArgsUtils.formatter)
      daysBeforeStartDate.foreach { day =>
        fs.delete(new Path(s"${baseDFS.deviceSignalPath}/dt=$day"), true)
      }

      // Write only new day data, as data from previous days don't change
      days
        .filterNot { day => fs.exists(new Path(s"${baseDFS.deviceSignalPath}/dt=$day")) }
        .foreach { day =>
          DataTables.deviceSignalTable(spark, settings.featuresDfs.signalEvents, day)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(s"${baseDFS.deviceSignalPath}/dt=$day")
        }

    }

    /////////////////////////// Hard Cache the Tables  ////////////////////////////////

    jobID match {
      case "salesTable"              => salesTable()
      case "salesPromoTable"         => salesPromoTable()
      case "salesPromoCategoryTable" => salesPromoCategoryTable()
      case "salesPromoMetaTable"     => salesPromoMetaTable()
      case "salesPromoRechTable"     => salesPromoRechTable()
      case "STRMerchantTable"        => strMerchantTable()
      case "gaAggTable"              => gaAggregatedTable()
      case "lms"                     => lmsTable()
      case "gaLPAggTable"            => gaAggregatedTableLP()
      case "strTable"                => strTable()
      case "deviceSignalTable"       => deviceSignalTable()
    }

  }

}
