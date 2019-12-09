package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.SparkJobValidations.{ArgLengthValidation, DailyJobValidation}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.utils.Settings
import com.paytm.map.features.utils.ArgsUtils
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import com.paytm.map.featuresplus.superuserid.Utility.{DFFunctions, DateBound, Functions}
import com.paytm.map.featuresplus.superuserid.Constants._
import com.paytm.map.featuresplus.superuserid.DataUtility._
import org.apache.spark.sql.types._

object BaseTable extends BaseTableJob with SparkJobBootstrap with SparkJob

trait BaseTableJob {
  this: SparkJob =>

  val JobName = "BaseTable"

  /**
   * No joining of any data is done here; only aggregation of individual source tables
   * Read all the source tables
   * make a Map(feature, sourceTable)
   * for a feature in Map: update / compute aggregations and write
   */

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args) && ArgLengthValidation.validate(args, 3)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    implicit val implicitSpark: SparkSession = spark
    implicit val implicitSettings: Settings = settings

    /** arguments **/
    val date = ArgsUtils.getTargetDate(args)
    val lookBack = args(1).toInt
    val dateRange = DateBound(date, lookBack)
    val isSubsequentRun = if (args(2) == "true") true else false
    val Path = new PathsClass(settings.featuresDfs.featuresPlusRoot)

    def delPhoneData(range: DateBound): DataFrame = {
      readSO(range).filter(DFFunctions.conditionPrimaryCol(Seq(col(delPhoneCol))).reduce(_ && _))
    }

    def bankAccountData(range: DateBound): DataFrame = {
      readWalletBank(range)
    }

    def addressData(range: DateBound): DataFrame = {
      readSOA(range).join(readSO(range), Seq("order_id"))
    }

    def deviceData(range: DateBound): DataFrame = {
      readDeviceData(range, Path)
    }

    def ruContactData(range: DateBound): DataFrame = {
      readFR(range).join(readSO(range), Seq("order_id"))
    }

    def merchantBankData(range: DateBound): DataFrame = {
      readMB(range).join(readEI, Seq("mid")).join(readUIDEID, Seq("entity_id"))
    }

    def delEmailData(range: DateBound): DataFrame = {
      readSO(range).filter(DFFunctions.conditionPrimaryCol(Seq(col(emailCol))).reduce(_ && _))
    }

    val featuresBaseDataMap: Map[String, DateBound => DataFrame] = Map(
      addressCol -> addressData,
      delPhoneCol -> delPhoneData,
      deviceCol -> deviceData,
      ruContactCol -> ruContactData,
      merchantBankCol -> merchantBankData,
      bankAccCol -> bankAccountData,
      emailCol -> delEmailData
    )

    /** main work */
    strongFeaturesOrder.foreach { feature =>
      println(feature)
      val featureCol = featuresConfigMap(feature).featureName
      val frequencyCol = featuresConfigMap(feature).frequency
      val featureWritePath = s"${Path.baseTablePath}/${featuresConfigMap(feature).path}"

      val groupedCols = Seq(customerIdCol, featureCol)

      val adjustedDateRange = if (isSubsequentRun) {
        // to make it fault tolerant from immediate previous run
        val latestAvailableDate =
          Functions.getLatestDate(featureWritePath, dateRange.startDate)
        val adjustedLookBack = Functions.dateDifference(latestAvailableDate, dateRange.endDate)
        DateBound(date, adjustedLookBack)
      } else {
        dateRange
      }

      println(s"from ${dateRange.startDate} to ${dateRange.endDate}")

      val updateBase = featuresBaseDataMap(feature)(adjustedDateRange)
      // make this generic for n columns
      val updateAgg = updateBase.
        groupBy(groupedCols.map(r => col(r)): _*).agg(count(customerIdCol).as(frequencyCol))
      val updateCols: Seq[String] = updateAgg.columns.filter(name => !groupedCols.contains(name))

      val snapshot: DataFrame = if (isSubsequentRun) {
        // needs updates wrt to old data
        val oldData = spark.read.parquet(s"$featureWritePath/dt=${adjustedDateRange.startDate}")
        updateAggregations(oldData, updateAgg, groupedCols, updateCols)
      } else {
        // simply new
        updateAgg
      }
      //      println(s"$feature has count so far at ${snapshot.count}")
      snapshot.na.drop("any").write.mode(SaveMode.Overwrite).parquet(s"$featureWritePath/dt=${adjustedDateRange.endDate}")

    }

    def updateAggregations(base: DataFrame, updates: DataFrame, groupedCols: Seq[String], updateCols: Seq[String]): DataFrame = {

      val baseRenamed = base.select(groupedCols.map(r => col(r)) ++ DFFunctions.addToColumnName(updateCols, "_old"): _*)
      val updateRenamed = updates.select(groupedCols.map(r => col(r)) ++ DFFunctions.addToColumnName(updateCols, "_update"): _*)

      // improve this mapping
      val defaultFillMap = (baseRenamed.schema ++ updateRenamed.schema).
        filter(item => !groupedCols.contains(item.name)).
        map { elem =>
          val name = elem.name
          val defaultValue = elem.dataType match {
            case IntegerType => 0
            case LongType    => 0.toLong
            case FloatType   => 0.toFloat
            case DoubleType  => 0.toDouble
            case _           => null
          }
          (name, defaultValue)
        }.toMap

      val updationRule = updateCols.map { r => (col(s"${r}_old") + col(s"${r}_update")).as(r) }

      val latestSnapshot = baseRenamed.join(updateRenamed, groupedCols, "outer").
        na.fill(defaultFillMap).
        select(groupedCols.map(r => col(r)) ++ updationRule: _*)

      latestSnapshot
    }

    /**
     * For each feature schema is:
     * |-- customer_id: long (nullable = true)
     * |-- delivery_phone: string (nullable = true)
     * |-- delivery_phone_frequency: long (nullable = true)
     */

  }
}

