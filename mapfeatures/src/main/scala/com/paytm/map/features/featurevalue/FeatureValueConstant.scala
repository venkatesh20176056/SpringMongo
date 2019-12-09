package com.paytm.map.features.featurevalue

import com.paytm.map.features.base.BaseTableUtils._
import com.paytm.map.features.datasets.DeviceOS
import com.paytm.map.features.featurevalue.FeatureValueConstant._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_set, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * Creates a Feature type column which has a dataype
 * and a method to calculate possible values for the feature
 */
trait FeatureColumn {
  val dataType: DataType

  /**
   * Returns a list of possible values of the feature
   *
   *  @param data dataframe containing the feature
   *  @param columnNames names of features that are of this datatype
   *  @return a Seq[String] of possible values of the feature
   */
  def getPossibleValues(data: DataFrame, columnNames: Seq[String]): Seq[String]
}

/**
 * Creates a String typed Feature column which has a String as it's dataype
 * and a method to calculate possible values by collecting the values as set
 */
class StringColumn extends FeatureColumn {
  override val dataType = StringType
  override def getPossibleValues(data: DataFrame, stringColumns: Seq[String]): Seq[String] = {
    val filteredData = data.select(stringColumns.head, stringColumns.tail: _*)
    val colletedData = getCollectSet(stringColumns, filteredData).cache
    getStringValues(stringColumns, colletedData)
  }
}

/**
 * Creates a List of String typed Feature column which has a List[String] as it's dataype
 * and a method to calculate possible values by exploding and finding
 * distinct of the values
 */
class ListStringColumn extends FeatureColumn {
  override val dataType = ArrayType(StringType)
  override def getPossibleValues(data: DataFrame, listColumns: Seq[String]): Seq[String] = {
    val filteredData = data.select(listColumns.head, listColumns.tail: _*).cache
    getListStringValues(listColumns, filteredData)
  }
}

/**
 * Create a Array of Nested typed Feature column which has a Array[Nested] as it's dataype
 * and a method to calculate possible values by selecting the nested fields and
 * exploding them and finding distinct of the values
 */
class NestedArrayColumn[A <: Product: TypeTag] extends FeatureColumn {
  val nestedSchema: StructType = Encoders.product[A].schema
  val requiredFields: Seq[String] = nestedSchema.fields.filter(_.dataType == StringType).map(_.name)

  override val dataType = ArrayType(nestedSchema)
  override def getPossibleValues(data: DataFrame, nestedArrayColumnName: Seq[String]): Seq[String] = {
    val fieldWithColumnNames = requiredFields.flatMap {
      fieldName => nestedArrayColumnName.map(colName => s"$colName.$fieldName")
    }

    val fieldWithColumnNamesCol = fieldWithColumnNames.map(colName => col(colName) as colName.replace(".", "_"))
    val filteredData = data.select(fieldWithColumnNamesCol: _*).cache

    getListStringValues(fieldWithColumnNames, filteredData)
  }
}

class NestedColumn[A <: Product: TypeTag] extends FeatureColumn {
  val nestedSchema = Encoders.product[A].schema
  val requiredFields: Seq[String] = nestedSchema.fields.filter(_.dataType == StringType).map(_.name)

  override val dataType = nestedSchema

  override def getPossibleValues(data: DataFrame, columnNames: Seq[String]): Seq[String] = {
    val fieldWithColumnNames = requiredFields.flatMap {
      fieldName => columnNames.map(colName => s"$colName.$fieldName")
    }

    getNestedStringValues(fieldWithColumnNames, data)
  }
}

object FeatureValueConstant {
  val logger = Logger.getLogger("FeatureValueLogger")
  logger.setLevel(Level.INFO)

  val POSSIBLE_VALUES_LIMIT = 1000

  val STRING_VALUES_LIMIT_EXCEPTIONS = Seq("city", "cleaned_city", "kyc_state", "kyc_city", "DEVICE_cell_network_provider")

  val colTypesToInclude: Seq[FeatureColumn] = Seq(
    new StringColumn,
    new ListStringColumn,
    new NestedArrayColumn[PromoUsage],
    new NestedArrayColumn[OperatorDueDate],
    new NestedArrayColumn[SMS],
    new NestedArrayColumn[DeviceOS],
    new NestedArrayColumn[BankCard],
    new NestedArrayColumn[TravelDetail],
    new NestedArrayColumn[CinemaVisited],
    new NestedArrayColumn[CineplexVisited],
    new NestedArrayColumn[CategorizedTxns],
    new NestedArrayColumn[MerchantSuperCashStatus],
    new NestedArrayColumn[MerchantEDCRejectedReason],
    new NestedArrayColumn[AppCategoryCnt],
    new NestedColumn[ExternalClientSignupDate]
  )

  def getCollectSet(stringColumns: Seq[String], features: DataFrame): DataFrame =
    {
      val agg = stringColumns.map {
        str => collect_set(col(str)) as str
      }

      features.select(stringColumns.head, stringColumns.tail: _*).agg(agg.head, agg.tail: _*)
    }

  def getStringValues(stringColumns: Seq[String], aggregatedData: DataFrame): Seq[String] = {

    stringColumns.flatMap {
      str: String =>
        {
          logger.info(s"Collecting values of string column: $str")
          val values = aggregatedData.select(str).rdd.map(r => r.getAs[Seq[String]](str)).first
          if (STRING_VALUES_LIMIT_EXCEPTIONS.contains(str) || (values.length <= POSSIBLE_VALUES_LIMIT && values.nonEmpty)) {
            Some(str + "\t" + values.mkString("\t"))
          } else {
            None
          }
        }
    }
  }

  def getListStringValues(listColumns: Seq[String], features: DataFrame): Seq[String] = {

    listColumns.flatMap {
      str: String =>
        {
          logger.info(s"Collecting values of list str column: $str")
          val newName = str.replace(".", "_")
          val values = features.select(explode(col(newName)) as newName).distinct
            .na.drop.rdd
            .map { case Row(a: String) => a }
            .collect()

          if ((newName == "installed_apps_list" && values.length <= 30000) || newName.contains("destination_city") || (values.length <= POSSIBLE_VALUES_LIMIT && values.nonEmpty)) {
            Some(str + "\t" + values.mkString("\t"))
          } else {
            None
          }
        }
    }
  }

  def getNestedStringValues(listColumns: Seq[String], features: DataFrame): Seq[String] = {
    listColumns.flatMap {
      colName: String =>
        {
          logger.info(s"Collecting values of nested string column: $colName")
          val values = features.select(colName).distinct
            .na.drop.rdd
            .map { case Row(a: String) => a }
            .collect()

          if (values.length <= POSSIBLE_VALUES_LIMIT && values.nonEmpty) {
            Some(colName + "\t" + values.mkString("\t"))
          } else {
            None
          }
        }
    }
  }

  def saveAsTabSepFile(spark: SparkSession, finalFileContent: Seq[String], savePath: String): Unit = {
    spark.sparkContext.parallelize(finalFileContent, 1).saveAsTextFile(savePath)
  }

}
