package com.paytm.map.features.export.elasticsearch

/**
 * https://www.elastic.co/guide/en/elasticsearch/hadoop/2.1/configuration.html#cfg-mapping
 *
 * @param esMappingId The document field/property name containing the document id.
 * @param esMappingParent The document field/property name containing the document parent.
 *                        To specify a constant, use the <CONSTANT> format.
 * @param esMappingVersion The document field/property name containing the document version.
 *                         To specify a constant, use the <CONSTANT> format.
 * @param esMappingVersionType Indicates the type of versioning used.
 *                             http://www.elastic.co/guide/en/elasticsearch/reference/2.0/docs-index_.html#_version_types
 *                             If es.mapping.version is undefined (default), its value is unspecified.
 *                             If es.mapping.version is specified, its value becomes external.
 * @param esMappingRouting The document field/property name containing the document routing.
 *                         To specify a constant, use the <CONSTANT> format.
 * @param esMappingTTLInMillis The document field/property name containing the document time-to-live.
 *                     To specify a constant, use the <CONSTANT> format.
 * @param esMappingTimestamp The document field/property name containing the document timestamp.
 *                           To specify a constant, use the <CONSTANT> format.
 */
case class EsDataFrameMappingConf(
  esMappingId: Option[String] = None,
  esMappingParent: Option[String] = None,
  esMappingVersion: Option[String] = None,
  esMappingVersionType: Option[String] = None,
  esMappingRouting: Option[String] = None,
  esMappingTTLInMillis: Option[String] = None,
  esMappingTimestamp: Option[String] = None
) extends Serializable

object EsDataFrameMappingConf {
  val CONSTANT_FIELD_REGEX = """\<([^>]+)\>""".r
}