package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.base.BaseTableUtils.dayIterator
import com.paytm.map.features.base.DataTables._
import com.paytm.map.features.config.Schemas.SchemaRepo.OnlineSchema
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object OnlineFeatures extends OnlineFeaturesJob
  with SparkJob with SparkJobBootstrap

trait OnlineFeaturesJob {
  this: SparkJob =>

  val JobName = "OnlineFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val dtSeq = dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    import spark.implicits._
    // make path
    val baseDFS = settings.featuresDfs.baseDFS
    val aggOnlinePath = s"${baseDFS.aggPath}/OnlineFeatures"
    val anchorPath = s"${baseDFS.anchorPath}/online"

    //Read Data
    val txnData = spark.read.parquet(baseDFS.paymentTxnPath).drop("alipay_trans_id") // dropping this col since it wasnt present here, this col was just added for avoiding datalake and features join in txnbase in measurements
      .where($"dt" >= dtSeq.min)
      .where($"customer_id".isNotNull)
      .where($"merchant_L1_type" === "ONLINE")
      .repartition($"dt", $"customer_id")

    //online Overall properties
    val onlineOverall = txnData
      .appendFeatures("merchant_L1_type", $"merchant_L1_type" === "ONLINE", "", "merchant_category", "cat", isMIDCol = true)

    // Merchant Specific Features.
    val mid2MerchantMapping = Seq(("actban35824809004371", "ACT_BRD"), ("actban41437920732443", "ACT_BRD"), ("actche81472742071381", "ACT_BRD"),
      ("actdel58237816762437", "ACT_BRD"), ("actelu20202291776115", "ACT_BRD"), ("actgun47280847595976", "ACT_BRD"),
      ("acthyd25472470736394", "ACT_BRD"), ("actnel61458152850778", "ACT_BRD"), ("acttir47877478021080", "ACT_BRD"),
      ("actvij32359526064830", "ACT_BRD"), ("actviz34410885124074", "ACT_BRD"), ("coimba64996488511181", "ACT_BRD"),
      ("ideupe03736738542317", "IDEA"), ("upepre46122941140811", "IDEA"), ("ideaap78290451538865", "IDEA"), ("ideaas20397212187719", "IDEA"),
      ("ideaas47188706969087", "IDEA"), ("ideabi46116968319934", "IDEA"), ("ideabi83681326614804", "IDEA"), ("ideadl34965968820496", "IDEA"),
      ("ideadl41278095929410", "IDEA"), ("ideagu09230024850687", "IDEA"), ("ideagu48822974776521", "IDEA"), ("ideaha62325349151311", "IDEA"),
      ("ideaha96642462177058", "IDEA"), ("ideahp22502267159784", "IDEA"), ("ideahp51334595045844", "IDEA"), ("ideajk47803125050851", "IDEA"),
      ("ideajk81238522255378", "IDEA"), ("ideaka09976156464122", "IDEA"), ("ideaka68201308909109", "IDEA"), ("ideake64180649324105", "IDEA"),
      ("ideako48786477961029", "IDEA"), ("ideako79861227247226", "IDEA"), ("ideama76205325133226", "IDEA"), ("ideama93539768770667", "IDEA"),
      ("ideamp17150920550772", "IDEA"), ("ideamp31466020423042", "IDEA"), ("ideamu54494357907926", "IDEA"), ("ideamu75811718720465", "IDEA"),
      ("ideane20843119400470", "IDEA"), ("ideane20974165394485", "IDEA"), ("ideaor08556155815648", "IDEA"), ("ideaor71344168913875", "IDEA"),
      ("ideapu23871350398772", "IDEA"), ("ideapu63580894888914", "IDEA"), ("ideapw57065763814619", "IDEA"), ("ideara12615798728077", "IDEA"),
      ("ideara25652289996587", "IDEA"), ("ideatn33473751992304", "IDEA"), ("ideatn94182935517727", "IDEA"), ("ideaup51205325225475", "IDEA"),
      ("ideaup84705682025782", "IDEA"), ("ideawb45023354977274", "IDEA"), ("ideawb62357737063335", "IDEA"), ("idkerp73865662237858", "IDEA"),
      ("ideaup80165887899580", "IDEA"), ("ideaup56855545842104", "IDEA"), ("ideker64577108236588", "IDEA"), ("ideamp87983543709710", "IDEA"),
      ("ideamu46508560740588", "IDEA"), ("relpre53281920366545", "JIO"), ("dream142155379103211", "DREAM11"), ("jublia80102820918555", "DOMINOS"),
      ("fooddd85493273063842", "FOODPANDA"), ("oodhmb12159180552579", "FOODPANDA"), ("foodvi47131202602207", "FRESHMENU"), ("swiggy58513599375770", "SWIGGY"),
      ("swiggy97751185767429", "SWIGGY"), ("uberea46906280393102", "UBEREATS"), ("zomato55495181157105", "ZOMATO"), ("bigbas01623248545487", "BIGBASKET"),
      ("grofer57242793868794", "GROFERS"), ("1mgtec11033447190383", "ONEMG"), ("health30728537335557", "HEALTHKART"), ("health77910468846906", "HEALTHKART"),
      ("medlif25017147919557", "MEDLIFE"), ("netmed28984116451023", "NETMEDS"), ("urbanc87893587009974", "URBANCLAP"), ("clfact07370583333746", "CLUBFACTORY"),
      ("firstc70950354973576", "FIRSTCRY"), ("novarr14858570160121", "JABONG"), ("groupo15109602040225", "NEARBUY"), ("xiacom77001433429391", "XIAOMI"),
      ("irctcw862137775", "IRCTC"), ("irctap18115103036330", "IRCTC"), ("irctcn13407115805523", "IRCTC"), ("irctce231206309", "IRCTC"), ("irctcu186290698", "IRCTC"),
      ("yxwyls51053157209367", "UBER"), ("oyoroo32665553977569", "OYO")).toMap

    val mid2MerchantUDF = udf((mid: String) => mid2MerchantMapping.getOrElse(mid, null))

    val merchantTaggedData = txnData
      .withColumn("merchantName", mid2MerchantUDF(lower($"mid")))
      .where($"merchantName".isNotNull)
      .groupBy("customer_id", "dt")
      .pivot("merchantName")
      .agg(
        count(lit(1)).as("tmp"), // to avoid spark bug where it fails to append
        count(col("order_item_id")).as("total_transaction_count")
      )
      .renameColumns("ONLINE_", excludedColumns = Seq("customer_id", "dt"))

    val merchantCategoryData = txnData
      .withColumn("merchant_category", lower($"merchant_category"))
      .withColumn("merchant_category", regexp_replace($"merchant_category", " ", "_"))
      .groupBy("customer_id", "dt")
      .pivot("merchant_category")
      .agg(
        count(lit(1)).as("tmp"),
        count(col("order_item_id")).as("total_transaction_count")
      )
      .renameColumns("ONLINE_", excludedColumns = Seq("customer_id", "dt"))

    merchantCategoryData.printSchema

    val dataDF = {
      onlineOverall
        .join(merchantTaggedData, Seq("customer_id", "dt"), "outer")
        .join(merchantCategoryData, Seq("customer_id", "dt"), "outer")
        .alignSchema(OnlineSchema)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write.partitionBy("dt")
        .mode(SaveMode.Overwrite)
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, aggOnlinePath)

  }

  private implicit class OnlineFeatImplicits(dF: DataFrame) {

    def appendFeatures(pivotCol: String, filter: Column, prefix: String, operatorCol: String = "", operatorStr: String = "", isMIDCol: Boolean = false): DataFrame = {

      val data = dF.where(filter)
      //Add Sales Features
      val salesFeat = data
        .addSalesAggregates(Seq("customer_id", "dt"), pivotCol, isDiscount = false)

      //Add FirstLast Features
      val firstLastData = if (operatorCol == "") data else data.withColumn("operator", col(operatorCol))
      val isOperator = if (operatorCol == "") false else true
      val firstLastFeat = firstLastData
        .addFirstLastCol(Seq("customer_id", "dt", pivotCol), isOperator)
        .addFirstLastAggregates(groupByCol = Seq("customer_id", "dt"), pivotCol, isOperator)
        .renameOperatorCol(operatorStr)

      //mid columns
      val midData = data
        .groupBy("customer_id", "dt")
        .pivot(pivotCol)
        .agg(
          countDistinct(col("mid")).as("total_txn_MID_count"),
          collect_set(col("mid")).as("all_txn_MID_list")
        )

      // Join Features
      val nonMidJoined = salesFeat.join(firstLastFeat, Seq("customer_id", "dt"), "inner")
      val joinedData = if (isMIDCol) nonMidJoined.join(midData, Seq("customer_id", "dt"), "inner") else nonMidJoined
      joinedData.renameColumns(prefix, excludedColumns = Seq("customer_id", "dt"))

    }

    def renameOperatorCol(operatorStr: String): DataFrame = {
      val newColNames = dF.columns.map { colM =>
        if (colM.endsWith("_transaction_operator")) colM.replace("operator", operatorStr) else colM
      }
      dF.toDF(newColNames: _*)
    }

  }

}