package com.paytm.map.features.export.elasticsearch

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.VersionType

/**
 * Extracts mappings from a Row for an IndexRequest.
 *
 * @param schema The StructType of a DataFrame.
 * @param mappingConf Configurations for IndexRequest.
 */
class EsDataFrameMapper(schema: StructType, mappingConf: EsDataFrameMappingConf) {
  import EsDataFrameMapper._
  import EsDataFrameMappingConf._

  def extractMappingId(currentRow: Row): Option[String] = {
    mappingConf.esMappingId.map(currentFieldName => currentRow.getAs[Any](currentFieldName).toString)
  }

  def extractMappingParent(currentRow: Row): Option[String] = {
    mappingConf.esMappingParent.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        currentRow.getAsToString(currentFieldName)
    }
  }

  def extractMappingVersion(currentRow: Row): Option[Long] = {
    mappingConf.esMappingVersion.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue.toLong
      case currentFieldName =>
        currentRow.getAsToString(currentFieldName).toLong
    }
  }

  def extractMappingVersionType(currentRow: Row): Option[VersionType] = {
    mappingConf.esMappingVersion
      .flatMap(_ => mappingConf.esMappingVersionType.map(VersionType.fromString))
  }

  def extractMappingRouting(currentRow: Row): Option[String] = {
    mappingConf.esMappingRouting.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        currentRow.getAsToString(currentFieldName)
    }
  }

  def extractMappingTTLInMillis(currentRow: Row): Option[Long] = {
    mappingConf.esMappingTTLInMillis.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue.toLong
      case currentFieldName =>
        currentRow.getAsToString(currentFieldName).toLong
    }
  }

  def extractMappingTimestamp(currentRow: Row): Option[String] = {
    mappingConf.esMappingTimestamp.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        currentRow.getAsToString(currentFieldName)
    }
  }
}

object EsDataFrameMapper {
  /**
   * Adds method to retrieve field as String through Any's toString.
   *
   * @param currentRow Row object.
   */
  implicit class RichRow(currentRow: Row) {
    // TODO: Find a better and safer way.
    def getAsToString(fieldName: String): String = {
      currentRow.getAs[Any](fieldName).toString
    }
  }
}