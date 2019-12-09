package com.paytm.map.features.utils

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.util.Try

object DataframeDelta {
  case class TableDeltaResult(deletedFields: Seq[String], insertedFields: Seq[String])
  case class RowDeltaResult(deletedRows: DataFrame, insertedRows: DataFrame)
  case class ColumnDeltaResult(updatedColumns: DataFrame)
  val ChangedFieldName = "columns_with_change"

  val mergeUDF =
    udf { (a: Seq[String], b: Seq[String]) =>
      (a, b) match {
        case (null, null) => null
        case (null, x)    => x
        case (x, null)    => x
        case _            => (a ++ b).distinct
      }
    }

  def TableDelta(prevDF: DataFrame, currDF: DataFrame) = {
    def fieldsNotExist(from: DataFrame, to: DataFrame) = {
      val currFieldMap = to.schema.fields.map(f => (f.name, f.dataType)).toMap
      from.schema.fields.filter(f =>
        currFieldMap.get(f.name) match {
          case Some(dataType) => !dataType.equals(f.dataType)
          case None           => true
        })
    }

    val deletedFields = fieldsNotExist(prevDF, currDF)
    val insertedFields = fieldsNotExist(currDF, prevDF)

    TableDeltaResult(deletedFields.map(_.name), insertedFields.map(_.name))
  }

  def RowDelta(prevDF: DataFrame, currDF: DataFrame, primaryKeys: Seq[String]) = {
    import prevDF.sparkSession.implicits._
    assertFieldNames(prevDF, primaryKeys)
    assertFieldNames(currDF, primaryKeys)

    val indicator = primaryKeys(0)
    val primaryColumns = primaryKeys.map(col(_))

    val primaryColumnsJoinCondition = joinKeysCondition(prevDF, currDF, primaryKeys)

    val full = prevDF.select(primaryColumns: _*).as("prev")
      .join(
        currDF.select(primaryColumns: _*).as("curr"),
        primaryColumnsJoinCondition, "full_outer"
      ).cache

    val currColumns = primaryKeys.map(key => (currDF(key)))
    val prevColumns = primaryKeys.map(key => (prevDF(key)))

    val insertedRows = full.where(prevDF(indicator) isNull).select(currColumns: _*)
    val deletedRows = full.where(currDF(indicator) isNull).select(prevColumns: _*)

    RowDeltaResult(deletedRows, insertedRows)
  }

  def ColumnDelta(prevDF: DataFrame, currDF: DataFrame, primaryKeys: Seq[String], tol: Double = 0.0) = {
    import prevDF.sparkSession.implicits._
    assertFieldNames(prevDF, primaryKeys)
    assertFieldNames(currDF, primaryKeys)

    val keyMap = primaryKeys.toSet
    val sharedFields = (prevDF.schema.fields.map(_.name).toSet intersect currDF.schema.fields.map(_.name).toSet)
      .filter(field => !keyMap.contains(field)).toSeq

    def boolFieldName(field: String) = s"_$field"
    val sharedRows = prevDF.join(currDF, primaryKeys)

    import scala.math.abs
    val condDF = {
      def changes(prevCols: Seq[Any], currCols: Seq[Any], colNames: Seq[String], tol: Double): Seq[String] = {

        def approxEqual(col1: Any, col2: Any, tol: Double): Boolean = {
          col1 match {
            case a1: Array[_] =>
              return approxEqual(a1.asInstanceOf[Seq[_]], col2.asInstanceOf[Seq[_]], tol)
            case s1: Seq[_] =>
              val s2 = col2.asInstanceOf[Seq[_]]
              if (s2 == null) return false
              if (s1.length != s2.length) return false
              if (s1.zip(s2).exists { case (a, b) => !approxEqual(a, b, tol) }) return false
            case f1: Float =>
              if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(col2.asInstanceOf[Float])) return false
              if (abs(f1 - col2.asInstanceOf[Float]) > tol) return false

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(col2.asInstanceOf[Double])) return false
              if (abs(d1 - col2.asInstanceOf[Double]) > tol) return false

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(col2.asInstanceOf[java.math.BigDecimal]) != 0) return false

            case _ =>
              if (col1 != col2) return false
          }
          true
        }

        (prevCols, currCols, colNames).zipped.withFilter {
          case (prevCol, currCol, _) =>
            !approxEqual(prevCol, currCol, tol)
        }.map(_._3).toSeq
      }

      val findChanges = udf(changes _)

      {
        val sharedFieldsSet = sharedFields.toSet
        prevDF.schema.fields.withFilter(f => sharedFieldsSet.contains(f.name)).map(f => (f.dataType, f.name)).groupBy(_._1).foldLeft(sharedRows) {
          case (df, fieldGroup) =>
            val fields = fieldGroup._2.map(_._2).toSeq
            val prevColumns = array(fields.map(prevDF(_)): _*)
            val currColumns = array(fields.map(currDF(_)): _*)
            val columnNames = array(fields.map(lit(_)): _*)

            val tmp = df.withColumn("tmp", findChanges(prevColumns, currColumns, columnNames, lit(tol)))

            if (Try(df(ChangedFieldName)).isSuccess) {
              tmp.withColumn(ChangedFieldName, mergeUDF(col(ChangedFieldName), $"tmp"))
            } else {
              tmp.withColumn(ChangedFieldName, $"tmp")
            }.drop("tmp")
        }
      }

    }.cache

    val primaryCols = primaryKeys.map(col(_))
    val delta = condDF
      .where(size(col(ChangedFieldName)) > 0)
      .select((primaryCols :+ col(ChangedFieldName)): _*)
    ColumnDeltaResult(delta)
  }

  def computeUpsert(prevDF: DataFrame, currDF: DataFrame, primaryKeys: Seq[String], tol: Double = 0.0) = {
    if (prevDF.count == 0) {
      currDF.withColumn(ChangedFieldName, lit(Array.empty[String]))
    } else {
      val primaryColumns = primaryKeys.map { col(_) }
      val outputSchema = (primaryColumns :+ col(ChangedFieldName))
      val insertedRows = RowDelta(prevDF, currDF, primaryKeys).insertedRows.withColumn(ChangedFieldName, lit(Array.empty[String]))
        .select(outputSchema: _*)

      val updatedRows = {
        val sharedRowsWithNewFields = {
          val newFields = TableDelta(prevDF, currDF).insertedFields.toArray
          if (newFields.length > 0) {
            prevDF.join(currDF, primaryKeys).select(primaryColumns: _*)
              .withColumn(ChangedFieldName, lit(newFields))
          } else {
            val newSchema = currDF.select(primaryColumns: _*).withColumn(ChangedFieldName, lit(newFields)).schema
            currDF.sparkSession.createDataFrame(currDF.sparkSession.sparkContext.emptyRDD[Row], newSchema)
          }
        }
        val rowsWithChanges = ColumnDelta(prevDF, currDF, primaryKeys, tol).updatedColumns

        sharedRowsWithNewFields.alias("t1").join(rowsWithChanges.alias("t2"), primaryKeys, "full_outer")
          .withColumn(s"_${ChangedFieldName}", mergeUDF(col(s"t1.${ChangedFieldName}"), col(s"t2.$ChangedFieldName")))
          .drop(col(s"t1.$ChangedFieldName")).drop(col(s"t2.$ChangedFieldName"))
          .withColumnRenamed(s"_${ChangedFieldName}", ChangedFieldName)
      }.select(outputSchema: _*)

      currDF.join((insertedRows union updatedRows).repartition(1000), primaryKeys)
    }
  }

  def computeDeletion(prevDF: DataFrame, currDF: DataFrame, primaryKeys: Seq[String], tol: Double = 0.0) = {
    val primaryColumns = primaryKeys.map { col(_) }
    val outputSchema = (primaryColumns :+ col(ChangedFieldName))

    val deletedRows = RowDelta(prevDF, currDF, primaryKeys)
      .deletedRows
      .withColumn(ChangedFieldName, lit(Array.empty[String]))
      .select(outputSchema: _*)

    val sharedRowsWithDeletedFields = {
      val deletedFields = TableDelta(prevDF, currDF).deletedFields.toArray
      if (deletedFields.length > 0) {
        prevDF.join(currDF, primaryKeys).select(primaryColumns: _*)
          .withColumn(ChangedFieldName, lit(deletedFields))
      } else {
        val newSchema = currDF.select(primaryColumns: _*).withColumn(ChangedFieldName, lit(deletedFields)).schema
        currDF.sparkSession.createDataFrame(currDF.sparkSession.sparkContext.emptyRDD[Row], newSchema)
      }
    }.select(outputSchema: _*)

    currDF.join((deletedRows union sharedRowsWithDeletedFields), primaryKeys)
  }

  private def assertFieldNames(df: DataFrame, fieldNames: Seq[String]) = {
    val fields = df.schema.fields.map(_.name)
    if (!fieldNames.forall(key => fields.contains(key)))
      throw new AssertionError(s"The key ${fieldNames} must presented in the dataframes")
  }

  private def joinKeysCondition(leftDF: DataFrame, rightDF: DataFrame, keys: Seq[String]) = {
    keys.map(key => (leftDF(key) <=> rightDF(key))).reduce((mergedCond, cond) => (mergedCond and cond))
  }
}
