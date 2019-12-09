package com.paytm.map.features.utils

import com.paytm.map.features.utils.GroupedDataframeOperations.getAdditionalColumns
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import com.paytm.map.features.utils.RowOperations._

import scala.annotation.tailrec

object GroupedDataframeOperations {

  sealed trait Column {
    def name: String
    def dataType: DataType
    def withPrefx(prefix: String): Column = SimpleColumn(prefix + "_" + name, dataType)
  }
  final case class SimpleColumn(override val name: String, override val dataType: DataType) extends Column
  // We may or may not wish to use a prefix when retrieving a column, hasPrefix is a flag to determine this intent.
  // ex:
  // 1) id, RU_AA_count_of_txns, timestamp
  // 2) id, RU_AA_count_of_txns, RU_AA_timestamp
  // The first example should use hasPrefix = false, and the second should use hasPrefix = true
  // If you wish the final result to have a different name, you can supply an option resultName value.
  final case class SelectColumn(override val name: String, override val dataType: DataType, hasPrefix: Boolean, resultName: String) extends Column
  object SelectColumn {
    def apply(name: String, dataType: DataType, hasPrefix: Boolean): SelectColumn = SelectColumn(name, dataType, hasPrefix, name)
  }
  final case class AdditionalSelectColumn(specialVerticals: Seq[String], selectColumns: Seq[SelectColumn])
  object AdditionalSelectColumn {
    val none = AdditionalSelectColumn(Seq.empty[String], Seq.empty[SelectColumn])
  }

  def getResultSchema(key: Column, selectColumns: Seq[SelectColumn], additionalColumns: AdditionalSelectColumn, verticals: Seq[String], levelPrefix: String): StructType = {
    StructType(
      StructField(key.name, key.dataType, false) +:
        verticals.flatMap { v =>
          selectColumns.map(c => StructField(addPrefix(v, c.resultName), c.dataType, true)) ++
            getAdditionalColumns(additionalColumns.specialVerticals, additionalColumns.selectColumns, v).map(c => StructField(addPrefix(v, c.resultName), c.dataType, true))
        }
    )
  }

  def addPrefix(vertical: String, name: String) = vertical + "_" + name

  /**
   * This method constructs a single result row by going over every row in the group, and applying a comparison operator
   * to retrieve the desired columns.
   *
   * TODO: create flatmap version of this function which returns Seq[Row], this way we can construct many result rows
   * for with different comparator functions, and different comparison columns.
   *
   * @param key - the key that the dataframe is grouped by
   * @param group - all the rows for a unique key
   * @param verticals
   * @param levelPrefix
   * @param comparisonColumnPostfix - the column to compare rows with
   * @param selectColumns - the columns to select from a row when the comparison deems a row to be preferred
   * @param comparator - a [[RowComparator]]
   * @param additionalColumns - some verticals may need to select more columns then others, the additional columns can be placed here
   * @return
   */
  def getColumnsUsingComparator[T](key: Column, group: Iterator[Row], verticals: Seq[String], levelPrefix: String, comparisonColumnPostfix: Column, selectColumns: Seq[SelectColumn], comparator: RowComparator[T], additionalColumns: AdditionalSelectColumn = AdditionalSelectColumn.none)(
    implicit
    ordering: Ordering[T]
  ): Row = {

    // This is the schema of the result row that we build up. It contains the key, the comparison columns, and the select columns.
    val aggSchema = StructType(
      StructField(key.name, key.dataType, false) +:
        verticals.flatMap(v =>
          StructField(addPrefix(v, comparisonColumnPostfix.name), comparisonColumnPostfix.dataType, true) +:
            (selectColumns.map(c => StructField(addPrefix(v, c.name), c.dataType, true)) ++
              getAdditionalColumns(additionalColumns.specialVerticals, additionalColumns.selectColumns, v).map(c => StructField(addPrefix(v, c.resultName), c.dataType, true))))
    )

    // Similar to the aggSchema, except now that all the comparisons have been made, and the result row is constructed,
    // we can drop the comparison columns, leaving just the key, and the select columns.
    val resultSchema = getResultSchema(key, selectColumns, additionalColumns, verticals, levelPrefix)

    /**
     * Select only the columns we are interested in, and enforce a schema.
     * An improvement would be to use an hlist to enforce type safety.
     * @param iterator
     * @return
     */
    def addSchemaToIterator(iterator: Iterator[Row]): Iterator[GenericRowWithSchema] = {
      iterator.map { row =>
        Seq(row.getAs(key.name, key.dataType)) ++
          verticals.flatMap { v =>
            Seq(row.getAs(addPrefix(v, comparisonColumnPostfix.name), comparisonColumnPostfix.dataType)) ++
              (selectColumns.map(c => row.getOrNull((if (c.hasPrefix) v + "_" else "") + c.name, c.dataType)) ++
                getAdditionalColumns(additionalColumns.specialVerticals, additionalColumns.selectColumns, v)
                .map(c => row.getOrNull((if (c.hasPrefix) v + "_" else "") + c.name, c.dataType)))
          }
      }.map(s => new GenericRowWithSchema(s.toArray, aggSchema))
    }

    /**
     * Build up a result row by comparing the comparison column of the result row with the next row in the iterator.
     * If a value from the new row is favoured by the comparison operation, select the select columns from this row to
     * be the new values in the result row.
     * @param iterator - contains all the records for a group
     * @param resultRow - the accumulator
     * @return - the accumulator
     */
    @tailrec
    def constructResultRow(iterator: Iterator[GenericRowWithSchema], resultRow: GenericRowWithSchema): GenericRowWithSchema = {
      if (iterator.hasNext) {
        val newRow = iterator.next()
        val results = verticals.zipWithIndex.map {
          case (v: String, i: Int) =>
            val comparisonColumn = comparisonColumnPostfix.withPrefx(v)
            val allSelectColumns = selectColumns ++ getAdditionalColumns(additionalColumns.specialVerticals, additionalColumns.selectColumns, v)
            // Apply the comparator for this specific vertical, if the new row is favoured, then we will update the
            // selectColumns for just this vertical.
            val resRow = comparator.compare(resultRow, newRow, comparisonColumn.name)

            // We only want to select the key column once, to avoid having multiple of the same key columns in the result set
            val keyColumn = if (i == 0) Seq(resRow.getAs(key.name, key.dataType)) else Seq()
            // Keep the comparison columns since we need to compare this result row with the next row in the iterator
            val comparisonColumns = Seq(resRow.getAs(comparisonColumn.name, comparisonColumn.dataType))
            // These are the current result values
            val resultSelectColumns = allSelectColumns.map(c => resRow.getAs(addPrefix(v, c.name), c.dataType))

            keyColumn ++ comparisonColumns ++ resultSelectColumns
        }
        constructResultRow(iterator, new GenericRowWithSchema(results.flatten.toArray, aggSchema))
      } else {
        val columnsOfInterest = key +: verticals.flatMap(v =>
          (selectColumns ++ getAdditionalColumns(additionalColumns.specialVerticals, additionalColumns.selectColumns, v))
            .map(c => SimpleColumn(addPrefix(v, c.name), c.dataType)))
        new GenericRowWithSchema(columnsOfInterest.map(c => resultRow.getAs(c.name, c.dataType)).toArray, resultSchema)
      }
    }

    val schemaIt = addSchemaToIterator(group)
    val head = schemaIt.next
    constructResultRow(schemaIt, head)
  }

  private def getAdditionalColumns(specialVerticals: Seq[String], additionalColumns: Seq[SelectColumn], vertical: String) = {
    if (specialVerticals.contains(vertical)) additionalColumns else Seq()
  }
}

/**
 * Given two [[GenericRowWithSchema]]'s, and a key, return one of the 2 rows by applying some comparison
 * @tparam T - the type of the column to compare (must have an implicit [[Ordering]])
 */
sealed trait RowComparator[T] {
  def compareIfNotNull(r0: GenericRowWithSchema, r1: GenericRowWithSchema, key: String, comparer: (T, T) => Boolean)(implicit ordering: Ordering[T]) =
    if (r1.isNullAt(r1.fieldIndex(key))) r0
    else if (r0.isNullAt(r0.fieldIndex(key))) r1
    else if (comparer(r0.getAs[T](key), r1.getAs[T](key))) r0
    else r1

  def compare(r0: GenericRowWithSchema, r1: GenericRowWithSchema, key: String)(implicit ordering: Ordering[T]): GenericRowWithSchema
}
case class LTE[T]() extends RowComparator[T] {
  def compare(r0: GenericRowWithSchema, r1: GenericRowWithSchema, key: String)(implicit ordering: Ordering[T]): GenericRowWithSchema =
    compareIfNotNull(r0, r1, key, ordering.lteq)
}
case class GTE[T]() extends RowComparator[T] {
  def compare(r0: GenericRowWithSchema, r1: GenericRowWithSchema, key: String)(implicit ordering: Ordering[T]): GenericRowWithSchema =
    compareIfNotNull(r0, r1, key, ordering.gteq)
}