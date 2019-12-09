package com.paytm.map.features.export.elasticsearch

import org.apache.spark.sql.DataFrame

/**
 * Extension of DataFrame for ES Bulkload
 *
 * @param dataFrame The DataFrame to lift into extension.
 */
class EsDataFrameFunctions(dataFrame: DataFrame) extends Serializable {

  val sparkContext = dataFrame.sparkSession.sparkContext

  /**
   * Insert DataFrame into Elasticsearch with the Java API utilizing a TransportClient.
   *
   * @param esIndex Index of DataFrame in Elasticsearch.
   * @param esType Type of DataFrame in Elasticsearch.
   * @param clientConf Configurations for the TransportClient.
   * @param mappingConf Configurations for IndexRequest.
   * @param writeConf Configurations for the BulkProcessor.
   *                  Empty by default.
   */
  def nativeInsertToEs(
    esIndex: String,
    esType: String,
    clientConf: EsNativeTransportClientConf,
    mappingConf: EsDataFrameMappingConf = EsDataFrameMappingConf(),
    writeConf: EsDataFrameWriteConf = EsDataFrameWriteConf()
  ): Unit = {
    val esNativeDataFrameWriter = new EsNativeDataFrameWriter(
      esIndex     = esIndex,
      esType      = esType,
      schema      = dataFrame.schema,
      clientConf  = clientConf,
      mappingConf = mappingConf,
      writeConf   = writeConf
    )

    sparkContext.runJob(dataFrame.rdd, esNativeDataFrameWriter.write _)
  }

  def nativeUpsertToEs(
    esIndex: String,
    esType: String,
    changeColumnName: String,
    clientConf: EsNativeTransportClientConf,
    mappingConf: EsDataFrameMappingConf = EsDataFrameMappingConf(),
    writeConf: EsDataFrameWriteConf = EsDataFrameWriteConf()
  ): Unit = {
    val esUpsertWriter = new EsNativeDataFrameUpsertWriter(
      esIndex          = esIndex,
      esType           = esType,
      schema           = dataFrame.schema,
      changeColumnName = changeColumnName,
      clientConf       = clientConf,
      mappingConf      = mappingConf,
      writeConf        = writeConf
    )

    sparkContext.runJob(dataFrame.rdd, esUpsertWriter.write _)
  }

  def restUpsertToEs(
    esIndex: String,
    esType: String,
    changeColumnName: String,
    clientConf: EsRestClientConf,
    mappingConf: EsDataFrameMappingConf = EsDataFrameMappingConf(),
    writeConf: EsDataFrameWriteConf = EsDataFrameWriteConf(),
    withUpsert: Boolean = false
  ): Unit = {
    val esUpsertWriter = new EsRestDataFrameUpsertWriter(
      esBaseIndex      = esIndex,
      esType           = esType,
      schema           = dataFrame.schema,
      changeColumnName = changeColumnName,
      clientConf       = clientConf,
      mappingConf      = mappingConf,
      writeConf        = writeConf,
      withUpsert       = withUpsert
    )

    sparkContext.runJob(dataFrame.rdd, esUpsertWriter.writeRDD _)
  }
}
