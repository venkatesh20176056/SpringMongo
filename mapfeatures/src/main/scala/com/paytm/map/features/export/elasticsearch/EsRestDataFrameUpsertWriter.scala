package com.paytm.map.features.export.elasticsearch

import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

/**
 * Utilizes Elasticsearch's RESTFul Java API to bulk load a DataFrame into Elasticsearch.
 *
 *
 * @param esBaseIndex The base index name of DataFrame in Elasticsearch.
 * @param esType Type of DataFrame in Elasticsearch.
 * @param schema StructType of DataFrame which corresponds to mapping in Elasticsearch.
 * @param changeColumnName the name of the column that contains the field that has changed
 * @param clientConf Configuration for the TransportClient.
 * @param mappingConf Configurations for IndexRequest.
 * @param writeConf Configurations for the BulkProcessor.
 */
class EsRestDataFrameUpsertWriter(
    esBaseIndex: String,
    esType: String,
    schema: StructType,
    changeColumnName: String,
    clientConf: EsRestClientConf,
    mappingConf: EsDataFrameMappingConf,
    writeConf: EsDataFrameWriteConf,
    withUpsert: Boolean = false
) extends Serializable with Logging {

  import EsRestDataFrameUpsertWriter.getCustomerIdIndex

  lazy val fieldStructTypeMap = {
    schema.fields.map(field => (field.name, field.dataType)).toMap
  }

  lazy val insertedSchema = {
    val insertedFields = schema.fields.withFilter(_.name != changeColumnName).map(_.name)
    StructType(insertedFields.map(f => StructField(f, fieldStructTypeMap(f))))
  }

  lazy val esMapper = {
    new EsDataFrameMapper(insertedSchema, mappingConf)
  }

  def write(data: Iterator[Row], taskId: Long = 0): Unit = {
    val localStartTime = System.nanoTime()

    val esClient = EsRestClientCache.getTransportClient(clientConf)

    def toBiConsumer[T, U](op: (T, U) => Unit): BiConsumer[T, U] = {
      new BiConsumer[T, U] {
        override def accept(t: T, u: U): Unit = op.apply(t, u)
      }
    }

    val bulkAsync = toBiConsumer[BulkRequest, ActionListener[BulkResponse]](esClient.bulkAsync(_, _))
    val esBulkProcessorListener = new EsDataFrameBulkProcessorListener(log, taskId)
    val esBulkProcessor = BulkProcessor.builder(bulkAsync, esBulkProcessorListener)
      .setBulkActions(writeConf.bulkActions)
      .setBulkSize(new ByteSizeValue(writeConf.bulkSizeInMB, ByteSizeUnit.MB))
      .setConcurrentRequests(writeConf.concurrentRequests)
      .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(writeConf.backoffInMillis), writeConf.numRetries
      ))
      .build()
    val indexNumber = getCustomerIdIndex(clientConf.numBuckets) _
    for (currentRow <- data) {
      esMapper.extractMappingId(currentRow).foreach { id =>
        val esIndex = s"${esBaseIndex}_${indexNumber(id)}"

        val nonEmptyData = insertedSchema
          .fields
          .map(f => (f, currentRow.getAs[Any](f.name)))
          .filter { case (_, v) => !filterNullCondition(v) }

        val nonEmptySchema = StructType(nonEmptyData.map(_._1))
        val nonEmptyRow = Row(nonEmptyData.map(_._2): _*)

        val esIndexSerializer = new EsDataFrameSerializer(nonEmptySchema, mappingConf)
        val source = esIndexSerializer.write(nonEmptyRow)

        val currentUpsertRequest = new IndexRequest(esIndex, esType, id).source(source)

        currentUpsertRequest.id(id)

        esMapper.extractMappingParent(currentRow).foreach { parent =>
          currentUpsertRequest.parent(parent)
        }
        esMapper.extractMappingVersion(currentRow).foreach { version =>
          currentUpsertRequest.version(version)
        }
        esMapper.extractMappingVersionType(currentRow).foreach { versionType =>
          currentUpsertRequest.versionType(versionType)
        }
        esMapper.extractMappingRouting(currentRow).foreach { routing =>
          currentUpsertRequest.routing(routing)
        }

        esBulkProcessor.add(currentUpsertRequest)
      }
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

  /**
   * Writes Rows to Elasticsearch by establishing a RestClient and BulkProcessor.
   *
   * @param taskContext The TaskContext provided by the Spark DAGScheduler.
   * @param data The set of Rows to persist.
   */
  def writeRDD(taskContext: TaskContext, data: Iterator[Row]): Unit = {
    write(data, taskContext.taskAttemptId())
  }

  def filterNullCondition(value: Any): Boolean = {
    value match {
      case v: Long if v == 0             => true
      case v: Double if v == 0           => true
      case v: Int if v == 0              => true
      case v: Float if v == 0            => true
      case v: Iterable[Any] if v.isEmpty => true
      case v if v == null                => true
      case _                             => false
    }
  }

}

object EsRestDataFrameUpsertWriter {
  def getCustomerIdIndex(totalIndices: Int = 10)(customerId: String): Int = {
    import scala.util.hashing.MurmurHash3
    val hashedCustomerId = Math.abs(MurmurHash3.stringHash(customerId))
    hashedCustomerId % totalIndices + 1
  }
}