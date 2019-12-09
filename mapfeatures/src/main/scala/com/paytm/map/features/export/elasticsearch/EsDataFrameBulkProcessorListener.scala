package com.paytm.map.features.export.elasticsearch

import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.slf4j.Logger

/**
 * Logs the executionId, number of requests, size, and latency of flushes.
 *
 * @param log Logger provided by Spark's Logging.
 */
class EsDataFrameBulkProcessorListener(log: Logger, taskAttemptId: Long) extends BulkProcessor.Listener {
  override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
    log.info(s"Task ($taskAttemptId); For executionId ($executionId), executing ${request.numberOfActions()} actions of estimate size ${request.estimatedSizeInBytes()} in bytes.")
  }

  // NOTE NEVER THROW EXCEPTION IN THIS METHOD!!! It will stop and retry will not function properly
  override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
    log.info(s"Task ($taskAttemptId); For executionId ($executionId), executed ${request.numberOfActions()} in ${response.getIngestTookInMillis} milliseconds.")
    if (response.hasFailures) {
      log.error(response.buildFailureMessage())
    }
  }

  // NOTE NEVER THROW EXCEPTION IN THIS METHOD!!! It will stop and retry will not function properly
  override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
    log.error(s"Task ($taskAttemptId); For executionId ($executionId), BulkRequest failed.", failure)
  }
}