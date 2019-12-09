package com.paytm.map.features.utils

import java.util.UUID

import com.paytm.map.features.base.BaseTableUtils.PromoUsage
import com.paytm.map.features.utils.DataTypeExtensions.DataTypeHelper
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.util.matching.Regex

object ConvenientFrame {

  implicit class LazyDataFrame(df: DataFrame) {
    val percentRegex: Regex = "(.*_percentage_.*)".r
    val countRegex: Regex = "(.*_transaction_count.*)".r
    val amountRegex: Regex = "(.*_transaction_size.*)".r
    val dateRegex: Regex = "(.*_date)".r
    val promoRegex: Regex = "(.*_promo_usage_dates)".r

    def forceTimestamp(tz: String = "Asia/Kolkata"): DataFrame = {
      val selectStatement = df.schema.map(column => {
        column.dataType match {
          case DateType | TimestampType => to_utc_timestamp(col(column.name).cast(TimestampType), tz).alias(column.name)
          case _                        => col(column.name)
        }
      })
      df.select(selectStatement: _*)
    }

    def filterGroupByAggregate(
      filters: Seq[String] = Seq(),
      groupByColumns: Seq[String] = Seq(),
      aggregations: Seq[(String, String)] = Seq()
    ): DataFrame = {

      val fnMapping: Map[String, Column => Column] = Map(
        "min" -> min,
        "max" -> max,
        "avg" -> avg,
        "count" -> count,
        "sum" -> sum,
        "collect_as_list" -> collect_list,
        "collect_as_set" -> collect_set,
        "count_distinct" -> approx_count_distinct
      )

      val allFilters = filters.mkString(" and ")
      val agg = aggregations.map {
        case (fnName, aggName) =>
          val colName = aggName.split(" as ")(0).trim
          val aliasName = aggName.split(" as ")(1).trim

          fnMapping(fnName)(col(colName)) as aliasName
      }

      val aggNames = aggregations.map {
        case (_, aggName) =>
          aggName.split(" as ")(0).trim
      }

      val filteredDF = df
        .transform { ds =>
          if (filters.nonEmpty) ds.filter(allFilters)
          else ds
        }

      val selectCols = groupByColumns ++ aggNames
      if (groupByColumns.nonEmpty) {
        filteredDF
          .forceSelect(selectCols: _*)
          .groupBy(groupByColumns.head, groupByColumns.tail: _*)
          .agg(agg.head, agg.tail: _*)
      } else filteredDF
    }

    def renameCols(columnNames: Seq[String]): DataFrame = {
      val renamedColumnNames = columnNames.map {
        columnName =>
          val colName = columnName.split(" as ")(0).trim
          val aliasName = columnName.split(" as ")(1).trim

          col(colName) as aliasName
      }
      val toBeReplacedCols = columnNames.map(_.split(" as ")(0).trim)
      val oldCols = df.columns.diff(toBeReplacedCols).map(col)
      val allCols = oldCols ++ renamedColumnNames
      df.select(allCols: _*)
    }

    // This function adds a suffix to all Cols except the excludedColumns(primary keys)
    def renameColsWithSuffix(suffix: String, excludedColumns: Seq[String]): DataFrame = {
      val renamedColumns = df.columns
        .map(c => if (excludedColumns.contains(c)) df(c).as(c) else df(c).as(s"$c$suffix"))
        .toSeq
      df.select(renamedColumns: _*)
    }

    def groupByAggregate(groupByColumns: Seq[String], aggregations: Seq[(String, String)]): DataFrame = {
      filterGroupByAggregate(groupByColumns = groupByColumns, aggregations = aggregations)
    }

    def filters(filters: Seq[String]): DataFrame = {
      filterGroupByAggregate(filters = filters)
    }

    def getFirstLast(
      partitionColumns: Seq[String],
      sortByColumn: String,
      selectColumns: Seq[String],
      isFirst: Boolean
    ): DataFrame = {
      val selectColumnWithoutAlias = selectColumns.map(aggName => aggName.split(" as ")(0).trim).filter(x => df.columns.contains(x))
      val selectFinalCoumns = selectColumns.filter(x => df.columns.contains(x.split(" as ")(0).trim))
      val finalCols = partitionColumns ++ selectColumnWithoutAlias

      val sortColumns = sortByColumn +: selectColumnWithoutAlias
      val sortColumnsWithOrder = {
        if (isFirst) sortColumns.map(col)
        else sortColumns.map(col(_).desc)
      }

      df
        .withColumn(
          "row_number",
          row_number.over(Window.partitionBy(partitionColumns.head, partitionColumns.tail: _*).orderBy(sortColumnsWithOrder: _*))
        )
        .filter(col("row_number") === "1")
        .drop("row_number")
        .forceSelect(finalCols: _*)
        .renameCols(selectFinalCoumns)
    }

    def castToCorrectType: DataFrame = {
      val cols = df.columns
      val correctTypes = cols.map { colName =>
        val fillValue = colName match {
          case "customer_id"                                 => LongType // For Athena
          case "is_email_verified"                           => LongType // For Athena
          case "SCB_inprogress_last_txn_date"                => df.schema("SCB_inprogress_last_txn_date").dataType //temp fix
          case "WALLET_SUBWALLET_last_balance_credited_date" => df.schema("WALLET_SUBWALLET_last_balance_credited_date").dataType // another temp fix
          case "external_sign_up_date"                       => df.schema("external_sign_up_date").dataType //yet another temp fix
          case percentRegex(`colName`)                       => DoubleType
          case countRegex(`colName`)                         => LongType
          case amountRegex(`colName`)                        => DoubleType
          case dateRegex(`colName`)                          => TimestampType // For Athena
          case promoRegex(`colName`)                         => ArrayType(Encoders.product[PromoUsage].schema)
          case other                                         => df.schema(other).dataType
        }
        col(colName).cast(fillValue)
      }
      df.select(correctTypes: _*)
    }

    def getNullsMap(cols: Seq[String]): Map[String, Any] = {
      cols.filter { colName =>
        Seq(LongType, DoubleType).contains(df.schema(colName).dataType)
      }
        .map { colName =>
          val fillValue = df.schema(colName).dataType match {
            case LongType   => 0L
            case DoubleType => 0.0
          }
          (colName, fillValue)
        }.toMap
    }

    def fillNulls: DataFrame = {
      df.na.fill(getNullsMap(df.columns))
    }

    def lowerStrings(colsToSkip: Set[String] = Set.empty[String]): DataFrame = {
      val cols = df.columns
      val loweredStringCols = cols.map { colName =>
        if (!colsToSkip.isEmpty && colsToSkip.contains(colName)) col(colName)
        else if (df.schema(colName).dataType == StringType) lower(col(colName)) as colName
        else col(colName)
      }
      df.select(loweredStringCols: _*)
    }

    val replaceStr = udf[String, String] {
      x => if (x == "") null else x
    }

    def replaceEmptyStrings: DataFrame = {
      val cols = df.columns
      val loweredStringCols = cols.map { colName =>
        if (df.schema(colName).dataType == StringType) replaceStr(col(colName)) as colName
        else col(colName)
      }
      df.select(loweredStringCols: _*)
    }

    /**
     * Rounds all double columns in the data frame to 3 decimal places
     */
    def roundDoubles: DataFrame = {
      val cols = df.columns
      val roundedCols = cols.map { colName =>
        if (df.schema(colName).dataType == DoubleType) round(col(colName), 3) as colName
        else col(colName)
      }
      df.select(roundedCols: _*)
    }

    def dropAll(cols: Seq[Column]): DataFrame = cols.foldLeft(df)((a, b) => a.drop(b))
    def dropAll(col: Column, cols: Column*): DataFrame = dropAll(col +: cols.toSeq)
    def dropAll(x: String, xs: String*): DataFrame = dropAll(col(x) +: xs.toSeq.map(col))

    /**
     * Selects the desired columns, if they don't exist, create them (null). This is to aid in
     * coercing schema in non-guaranteed schema'd input
     *
     * @param cols
     * @return
     */
    def forceSelect(cols: String*): DataFrame = {
      val available = df.columns.toSet
      val coerced = cols.foldLeft(Seq[Column]()) {
        case (accum, c) =>
          val resolved = if (available.contains(c)) col(c) else lit(null).cast(LongType) as c
          resolved +: accum
      }.reverse

      df.select(coerced: _*)
    }

    /**
     * Wrapper method for DataFrame unionAll method that will assert that the schemas are the same before performing the union
     *
     * @param other DataFrame to union
     * @param strict If true, it will assert that the column names and the types are the same (default), otherwise will just assert types are the same
     * @return Unioned DataFrame
     * @throws IllegalArgumentException RunTime error if union is not in the desired nature (Damn you non-type-safe df!)
     */
    def safeUnion(other: DataFrame, strict: Boolean = true): DataFrame = {

      if (strict) {
        require(
          df.dtypes.mkString == other.dtypes.mkString,
          s"""
              | Column names and types must match when attempting a strict safe union.
              | left:  ${df.dtypes.mkString("|")}
              | right: ${other.dtypes.mkString("|")}
              |"""".stripMargin
        )
      } else {
        require(
          df.dtypes.map(_._2).mkString == other.dtypes.map(_._2).mkString,
          s"""
              | Types must match when performing a safe union.
              | left:  ${df.dtypes.map(_._2).mkString("|")}
              | right: ${other.dtypes.map(_._2).mkString("|")}
              |""".stripMargin
        )
      }

      df.union(other)
    }

    def skewJoin(
      other: DataFrame,
      skewedColName: String, joinColumns: Seq[String], joinType: String, n: Int = 10000
    ): DataFrame = {
      // taking first n[default: 10000] skewed values
      val maxRows = df.groupBy(skewedColName).count().sort(desc("count")).take(n)
      val skewVals: Seq[Any] = maxRows.map(_.get(0))

      // filtering the skewed rows and performing broadcast join.
      val broadcastDataFrame1 = df.filter(col(skewedColName).isin(skewVals: _*))
      val broadcastDataFrame2 = other.filter(col(skewedColName).isin(skewVals: _*))
      val broadcastJoinDataFrame = broadcastDataFrame1.join(broadcast(broadcastDataFrame2), joinColumns, joinType)

      // filtering the non skewed rows and performing simple shuffle join.
      val shuffleDataFrame1 = df.filter(!col(skewedColName).isin(skewVals: _*))
      val shuffleDataFrame2 = other.filter(!col(skewedColName).isin(skewVals: _*))
      val shuffleJoinDataFrame = shuffleDataFrame1.join(shuffleDataFrame2, joinColumns, joinType)

      // union of broadcast join and shuffle join
      broadcastJoinDataFrame union shuffleJoinDataFrame
    }

    def withColumnReplaced(name: String, udf: Column): DataFrame = {
      val tmp = UUID.randomUUID.toString
      df.withColumn(tmp, udf)
        .drop(name)
        .withColumnRenamed(tmp, name)
    }

    def matchSchema(expectedSchema: StructType): Boolean = {

      df.schema.treeString == expectedSchema.treeString
    }

    def matchSchemaSubset(expectedSchema: StructType): Boolean = {

      expectedSchema.toSet subsetOf df.schema.toSet
    }

    /**
     * Case when function that creates new columns based on a list of conditions to help with aggregations.
     *
     * @param nameCondIfTrue the new column name, the condition in the form of a column,
     * and the value to enter in new column if the condition is true (null if false).
     */

    def caseWhen(nameCondIfTrue: Seq[(String, Column, Column)]): DataFrame = {
      val newCols = nameCondIfTrue.map {
        case (colName: String, condition: Column, ifTrue: Column) => when(condition, ifTrue).as(colName)
      }
      df.select(col("*") +: newCols: _*)
    }

    def columnLookbackFilter(name: String, dataCol: String, dtCol: String, lookbackDays: Int, targetDateStr: String): DataFrame = {
      val condition = col(dtCol) > date_add(lit(targetDateStr), -lookbackDays)
      val nameCondIfTrue = (name, condition, col(dataCol))
      caseWhen(Seq(nameCondIfTrue))
    }

    /**
     * UDF to order alphabetically and replace nulls for use in transformListCols function
     */
    val processList = udf((listCol: Seq[String]) => {
      if (listCol.isEmpty) null
      else listCol.sorted
    })

    /**
     * Orders alphabetically and replaces empty arrays with nulls any Array columns
     */
    def transformListCols: DataFrame = {
      val arrayCols = df.schema.map { field =>
        field.dataType match {
          case _: ArrayType => when(df(field.name).isNotNull, processList(df(field.name))) as field.name
          case _            => df(field.name)
        }
      }
      df.select(arrayCols: _*)
    }

    /**
     * Changes spark-sql TimestampType columns to unix timestamp long type
     */
    def transformTimestampCols: DataFrame = {
      val timestampCols = df.schema.map { field =>
        field.dataType match {
          case _: TimestampType => when(df(field.name).isNotNull, unix_timestamp(df(field.name))) as field.name
          case _                => df(field.name)
        }
      }
      df.select(timestampCols: _*)
    }

    /**
     * Changes array type to string list for writing to JDBC database
     */
    val listToString = udf((listCol: Seq[String]) => listCol match {
      case null => None
      case list => Some(list.mkString("; "))
    })

    val nestedToString = udf((listCol: Seq[StructType]) => listCol match {
      case null => None
      case list => Some(list.mkString(", "))
    })

    def listColsToString: DataFrame = {
      val cols = df.schema.map { field =>
        field.dataType match {
          case ArrayType(StringType, _) => listToString(df(field.name)) as field.name
          case _: ArrayType             => nestedToString(df(field.name)) as field.name
          case _                        => df(field.name)
        }
      }
      df.select(cols: _*)
    }

    def addPrefixToColumns(prefix: String, excludedColumns: Seq[String] = Seq()): DataFrame = {
      addPrefixSuffixToColumns(prefix = prefix, excludedColumns = excludedColumns)
    }

    def addSuffixToColumns(suffix: String, excludedColumns: Seq[String] = Seq()): DataFrame = {
      addPrefixSuffixToColumns(suffix = suffix, excludedColumns = excludedColumns)
    }

    def addPrefixSuffixToColumns(prefix: String = "", suffix: String = "", excludedColumns: Seq[String]): DataFrame = {
      val columnsRenamed = df.columns
        .map(colM =>
          if (excludedColumns.contains(colM)) df.col(colM)
          else df.col(colM).as(prefix + colM + suffix)).toSeq
      df.select(columnsRenamed: _*)
    }

    /**
     * Checks whether info of specified column was verified before target date and adds 1-0 flag column
     */
    def infoVerified(newColName: String, verifiedField: Column, targetDate: String) = {
      df.withColumn(newColName, when(verifiedField.isNotNull && verifiedField < targetDate, 1).otherwise(0))
    }

    def getColumnsNamesOfType(dType: DataType): Seq[String] = {
      df.schema.fields.filter(_.dataType.sameType(dType)).map(_.name)
    }

    def saveParquet(path: String, noOfPartitions: Int = 200, withRepartition: Boolean = true) = {
      val writeDF = if (withRepartition) df.repartition(noOfPartitions) else df
      writeDF.write.mode(SaveMode.Overwrite).format("parquet").save(path)
    }

    def saveEsSimple(esUrl: String, port: Int, esIndex: String, esType: String, mapping_id: String) = {
      df
        .write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes.wan.only", "true")
        .option("es.port", port)
        .option("es.net.ssl", "true")
        .option("es.nodes", esUrl)
        .option("es.mapping_id", mapping_id)
        .mode("overwrite")
        .save(s"$esIndex/$esType")
    }

    def saveEsUpsert(esUrl: String, port: Int, esIndex: String, esType: String, mapping_id: String) = {
      df
        .write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes.wan.only", "true")
        .option("es.port", port)
        .option("es.net.ssl", "true")
        .option("es.nodes", esUrl)
        .option("es.mapping_id", mapping_id)
        .option("es.write.operation", "upsert")
        .option("es.mapping.date.rich", "false")
        .mode("append")
        .save(s"$esIndex/$esType")
    }

    def saveCSV(path: String): Unit = df.write.mode(SaveMode.Overwrite).csv(path)
    def saveCSV(path: String, noOfPartitions: Int = 200): Unit = df.repartition(noOfPartitions).write.mode(SaveMode.Overwrite).csv(path)

    def saveRedshift(redshiftUrl: String, redshiftTable: String, tempPath: String, mode: String = "append") = {

      df.write
        .mode(mode)
        .format("com.databricks.spark.redshift")
        .option("url", redshiftUrl)
        .option("tempdir", tempPath)
        .option("dbtable", redshiftTable)
        .save()
    }

    def toSparseDF: DataFrame = {
      val schema = df.schema
      val sparseRDD = df.rdd.map(row => SparseRow.create(schema, row.toSeq: _*))
      df.sparkSession.sqlContext.createDataFrame(sparseRDD, schema)
    }

    def coalescePartitions(partitionCol: String, secondaryCol: String, partitionValues: Seq[String], partitionsPerDay: Int = 10): DataFrame = {
      if (partitionValues.isEmpty)
        df
      else
        df
          .where(col(partitionCol).between(partitionValues.min, partitionValues.max))
          .repartitionByRange(partitionValues.size * partitionsPerDay, col(partitionCol), col(secondaryCol))
    }
  }

  implicit class seqOfFrames(dfs: Seq[DataFrame]) {
    def getNonEmpty = {
      dfs.filter(df => !df.head(1).isEmpty)
    }

    def joinAllAndRepartiton(keyColumnNames: Seq[String], joinType: String, partitions: Int = 500) = {
      dfs.reduce((a, b) => a.join(b, keyColumnNames, joinType).repartition(partitions))
    }

    def joinAllWithoutRepartiton(keyColumnNames: Seq[String], joinType: String) = {
      dfs.reduce((a, b) => a.join(b, keyColumnNames, joinType))
    }

    def joinAll(keyColumnNames: Seq[String], joinType: String,
      partitions: Int = 500,
      withRepartition: Boolean = true) = {
      if (withRepartition) joinAllAndRepartiton(keyColumnNames, joinType, partitions)
      else joinAllWithoutRepartiton(keyColumnNames, joinType)
    }

    def joinAllSmall(keyColumnNames: Seq[String], joinType: String) = {
      dfs.reduce((a, b) => a.join(b, keyColumnNames, joinType))
    }

    def unionAll: DataFrame = {
      dfs.reduce((a, b) => a.union(b))
    }
  }

  def configureSparkForMumbaiS3Access(spark: SparkSession) {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
  }

  def configureSparkForSingaporeS3Access(spark: SparkSession) {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com")
  }
}

