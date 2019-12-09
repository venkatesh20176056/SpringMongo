package com.paytm.map.featuresplus.rechargeprediction

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.featuresplus.rechargeprediction.Utility._
import org.apache.spark.sql.functions._

object BaseTable extends BaseTableJob with SparkJobBootstrap with SparkJob

trait BaseTableJob {
  this: SparkJob =>
  val JobName = "RechargePredictionBaseTable"
  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    /** Date Utils **/
    val date: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays = Constants.lookBackDays
    val dateRange = DateBound(date, lookBackDays)
    val basePath = Constants.projectBasePath(settings.featuresDfs.featuresPlusRoot)
    val rawDataWritePath = s"$basePath/${Constants.rawDataPath}/date=${dateRange.endDate}"

    if (!Function.checkFileExists(spark, rawDataWritePath)) {
      /** Reading Data **/
      val so = DataTables.soTable(spark, settings.datalakeDfs.salesOrder, dateRange.endDate, dateRange.startDate).
        filter(TableCondition.soConfig.rowsConditions).
        select(TableCondition.soConfig.colsCondition: _*)

      val soi = DataTables.soiTable(spark, settings.datalakeDfs.salesOrderItem, dateRange.endDate, dateRange.startDate).
        filter(TableCondition.soiConfig.rowsConditions).
        select(TableCondition.soiConfig.colsCondition: _*)

      val recharge = DataTables.rechargeTableWithNum(spark, settings.datalakeDfs.fulfillementRecharge, dateRange.endDate, dateRange.startDate).
        filter(TableCondition.rechargeConfig.rowsConditions).
        select(TableCondition.rechargeConfig.colsCondition: _*)

      val profilePath = Constants.profilePath(s"${settings.featuresDfs.featuresTable}/${settings.featuresDfs.profile}")

      val latestAvailableProfile: String =
        Function.getLatest(profilePath, dateRange.endDate, "/dt=")
      println(s"latestDate: $latestAvailableProfile")

      val profile = spark.read.parquet(latestAvailableProfile).
        filter(TableCondition.profileConfig.rowsConditions).
        select(TableCondition.profileConfig.colsCondition: _*).
        distinct()

      val rawData = so.
        join(soi, Seq("order_id")).
        join(recharge, Seq("order_item_id")).
        join(profile, Seq("customer_id")).
        withColumn("is_ben_same", col("reg_phone_no").contains(col("ben_phone_no")))

      /** final writing/ dumping of data **/
      rawData.write.mode(SaveMode.Overwrite).parquet(rawDataWritePath)

    }

  }

  // deprecated after reduction of lookBackDays
  @deprecated
  def writeIncrementalData(spark: SparkSession, data: DataFrame, colToPartition: String, writePath: String, dummyWritePath: String): Unit = {
    data.write.partitionBy(colToPartition).mode(SaveMode.Overwrite).parquet(dummyWritePath)
    val dataPartitioned = spark.read.parquet(dummyWritePath)
    dataPartitioned.select(colToPartition).distinct.rdd.map(r => r(0).toString).foreach(
      value =>
        dataPartitioned.
          filter(col(colToPartition) === value).
          write.mode(SaveMode.Overwrite).
          parquet(s"$writePath/$colToPartition=$value")
    )
  }

  """
    |scala> baseTable.printSchema
    |root
    | |-- ben_phone_no: string (nullable = true)
    | |-- operator: string (nullable = true)
    | |-- circle: string (nullable = true)
    | |-- date: date (nullable = true)
    | |-- amount: double (nullable = true)
    | |-- count: long (nullable = false)
    | |-- discount: double (nullable = true)
    | |-- countSelfRecharge: long (nullable = false)
    | |-- age: integer (nullable = true)
    | |-- is_email_verified: string (nullable = true)
    |
    """.stripMargin

}
