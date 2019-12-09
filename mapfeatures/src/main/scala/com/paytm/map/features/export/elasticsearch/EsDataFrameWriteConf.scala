package com.paytm.map.features.export.elasticsearch

/**
 * Configurations for EsNativeDataFrameWriter's BulkProcessor.
 *
 * @param bulkActions The number of IndexRequests to batch in one request.
 * @param bulkSizeInMB The maximum size in MB of a batch.
 * @param concurrentRequests The number of concurrent requests in flight.
 * @param flushTimeoutInSeconds The maximum time in seconds to wait while closing a BulkProcessor.
 */
case class EsDataFrameWriteConf(
  bulkActions: Int = 1000,
  bulkSizeInMB: Int = 5,
  concurrentRequests: Int = 1,
  flushTimeoutInSeconds: Long = 10,
  backoffInMillis: Long = 500L,
  numRetries: Int = 10
) extends Serializable