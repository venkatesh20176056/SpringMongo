package com.paytm.map.features.export.elasticsearch

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

/**
 * Serializes a Row from a DataFrame into a XContentBuilder.
 *
 * @param schema The StructType of a DataFrame.
 * @param mappingConf Configurations for IndexRequest.
 */
class EsDataFrameSerializer(schema: StructType, mappingConf: EsDataFrameMappingConf) {
  /**
   * Serializes a Row from a DataFrame into a XContentBuilder.
   *
   * XContentBuilder provides the following:
   *   - bytes().
   *   - stream().
   *   - string().
   *
   * @param value A Row.
   * @return A XContentBuilder.
   */
  def write(value: Row): XContentBuilder = {
    val currentJsonBuilder = XContentFactory.jsonBuilder()

    write(schema, value, currentJsonBuilder)

    currentJsonBuilder
  }

  private def write(dataType: DataType, value: Any, builder: XContentBuilder): XContentBuilder = {
    dataType match {
      case structType @ StructType(_)  => writeStruct(structType, value, builder)
      case arrayType @ ArrayType(_, _) => writeArray(arrayType, value, builder)
      case mapType @ MapType(_, _, _)  => writeMap(mapType, value, builder)
      case _                           => writePrimitive(dataType, value, builder)
    }
  }

  private def writeStruct(structType: StructType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case currentRow: Row =>
        builder.startObject()

        structType.fields.view.zipWithIndex foreach {
          case (field, index) =>
            builder.field(field.name)
            if (currentRow.isNullAt(index)) {
              builder.nullValue()
            } else {
              write(field.dataType, currentRow(index), builder)
            }
        }

        builder.endObject()
    }

    builder
  }

  private def writeArray(arrayType: ArrayType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case array: Array[_] =>
        serializeArray(arrayType.elementType, array, builder)
      case seq: Seq[_] =>
        serializeArray(arrayType.elementType, seq, builder)
      case _ =>
        throw new IllegalArgumentException(s"Unknown ArrayType: $value.")
    }
  }

  private def serializeArray(dataType: DataType, value: Seq[_], builder: XContentBuilder): XContentBuilder = {
    // TODO: Consider utilizing builder.value(Iterable[_]).
    builder.startArray()

    if (value != null) {
      value foreach { element =>
        write(dataType, element, builder)
      }
    }

    builder.endArray()
    builder
  }

  private def writeMap(mapType: MapType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case scalaMap: scala.collection.Map[_, _] =>
        serializeMap(mapType, scalaMap, builder)
      case javaMap: java.util.Map[_, _] =>
        serializeMap(mapType, javaMap.asScala, builder)
      case _ =>
        throw new IllegalArgumentException(s"Unknown MapType: $value.")
    }
  }

  private def serializeMap(mapType: MapType, value: scala.collection.Map[_, _], builder: XContentBuilder): XContentBuilder = {
    // TODO: Consider utilizing builder.value(Map[_, AnyRef]).
    builder.startObject()

    for ((currentKey, currentValue) <- value) {
      builder.field(currentKey.toString)
      write(mapType.valueType, currentValue, builder)
    }

    builder.endObject()
    builder
  }

  private def writePrimitive(dataType: DataType, value: Any, builder: XContentBuilder): XContentBuilder = {
    dataType match {
      case BinaryType    => builder.value(value.asInstanceOf[Array[Byte]])
      case BooleanType   => builder.value(value.asInstanceOf[Boolean])
      case ByteType      => builder.value(value.asInstanceOf[Byte])
      case ShortType     => builder.value(value.asInstanceOf[Short])
      case IntegerType   => builder.value(value.asInstanceOf[Int])
      case LongType      => builder.value(value.asInstanceOf[Long])
      case DoubleType    => builder.value(value.asInstanceOf[Double])
      case FloatType     => builder.value(value.asInstanceOf[Float])
      case TimestampType => builder.value(value.asInstanceOf[Timestamp])
      case DateType      => builder.value(value.asInstanceOf[Date])
      case StringType    => builder.value(value.toString)
      case _ =>
        // TODO: Possibly, warn that ElasticSearch does not support the DecimalType.
        throw new IllegalArgumentException(s"Unknown DataType: $dataType for value: $value.")
    }
  }
}