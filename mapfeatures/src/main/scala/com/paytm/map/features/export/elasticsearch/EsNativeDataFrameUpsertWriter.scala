package com.paytm.map.features.export.elasticsearch

import java.util.concurrent.TimeUnit

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue}

/**
 * Utilizes Elasticsearch's native Java API to bulk load a DataFrame into Elasticsearch.
 *
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/transport-client.html
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/java-docs-bulk.html
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/java-docs-bulk-processor.html
 *
 * @param esIndex Index of DataFrame in Elasticsearch.
 * @param esType Type of DataFrame in Elasticsearch.
 * @param schema StructType of DataFrame which corresponds to mapping in Elasticsearch.
 * @param clientConf Configuration for the TransportClient.
 * @param mappingConf Configurations for IndexRequest.
 * @param writeConf Configurations for the BulkProcessor.
 */
@deprecated
class EsNativeDataFrameUpsertWriter(
    esIndex: String,
    esType: String,
    schema: StructType,
    changeColumnName: String,
    clientConf: EsNativeTransportClientConf,
    mappingConf: EsDataFrameMappingConf,
    writeConf: EsDataFrameWriteConf
) extends Serializable with Logging {
  lazy val esIndexSerializer = {
    new EsDataFrameSerializer(schema, mappingConf)
  }
  lazy val esMapper = {
    new EsDataFrameMapper(schema, mappingConf)
  }
  lazy val fieldStructTypeMap = {
    schema.fields.map(field => (field.name, field.dataType)).toMap
  }

  /**
   * Writes Rows to Elasticsearch by establishing a TransportClient and BulkProcessor.
   *
   * @param taskContext The TaskContext provided by the Spark DAGScheduler.
   * @param data The set of Rows to persist.
   */
  def write(taskContext: TaskContext, data: Iterator[Row]): Unit = {
    val localStartTime = System.nanoTime()

    val esClient = EsNativeTransportClientCache.getTransportClient(clientConf)

    val esBulkProcessorListener = new EsDataFrameBulkProcessorListener(log, taskContext.taskAttemptId())
    val esBulkProcessor = BulkProcessor.builder(esClient, esBulkProcessorListener)
      .setBulkActions(writeConf.bulkActions)
      .setBulkSize(new ByteSizeValue(writeConf.bulkSizeInMB, ByteSizeUnit.MB))
      .setConcurrentRequests(writeConf.concurrentRequests)
      .build()

    for (currentRow <- data) {
      val currentIndexRequest = new IndexRequest(esIndex, esType) source {
        esIndexSerializer.write(currentRow)
      }

      val id = esMapper.extractMappingId(currentRow).getOrElse(throw new RuntimeException(s"Key must be provided: ${mappingConf}"))

      // Configure the Index Request
      currentIndexRequest.id(id)
      esMapper.extractMappingParent(currentRow).foreach(currentIndexRequest.parent)
      esMapper.extractMappingVersion(currentRow).foreach(currentIndexRequest.version)
      esMapper.extractMappingVersionType(currentRow).foreach(currentIndexRequest.versionType)
      esMapper.extractMappingRouting(currentRow).foreach(currentIndexRequest.routing)

      // Configure the Update Request
      val currentUpsertRequest = {
        val (docSerializer, updateRow) = {
          val updatedFields = currentRow.getAs[Seq[String]](changeColumnName)
          val docSchema = StructType(updatedFields.map(f => StructField(f, fieldStructTypeMap(f))))
          val serializer = new EsDataFrameSerializer(docSchema, mappingConf)
          val updateRow = Row(updatedFields.map(f => currentRow.get(currentRow.fieldIndex(f))): _*)
          (serializer, updateRow)
        }

        new UpdateRequest(esIndex, esType, id)
          .doc(docSerializer.write(updateRow))
          .upsert(currentIndexRequest)
      }

      currentUpsertRequest.id(id)
      esMapper.extractMappingParent(currentRow).foreach(currentUpsertRequest.parent)
      esMapper.extractMappingVersion(currentRow).foreach(currentUpsertRequest.version)
      esMapper.extractMappingVersionType(currentRow).foreach(currentUpsertRequest.versionType)
      esMapper.extractMappingRouting(currentRow).foreach(currentUpsertRequest.routing)

      esBulkProcessor.add(currentUpsertRequest)
    }

    val isClosed = esBulkProcessor.awaitClose(writeConf.flushTimeoutInSeconds, TimeUnit.SECONDS)
    if (isClosed) {
      log.info("Closed Elasticsearch Bulk Processor.")
    } else {
      log.error("Elasticsearch Bulk Processor failed to close.")
    }

    val localEndTime = System.nanoTime()
    val differenceTime = localEndTime - localStartTime
    log.info(s"Elasticsearch Task completed in ${TimeUnit.MILLISECONDS.convert(differenceTime, TimeUnit.NANOSECONDS)} milliseconds.")
  }
}