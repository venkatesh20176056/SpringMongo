package com.paytm.map.features.config
import com.typesafe.config.Config

case class EsExportConfig(
  numActiveUsers: Int,
  numBuckets: Int,
  backoffInMillis: Long,
  numRetries: Int
)

object EsExportConfig {
  def apply(config: Config): EsExportConfig = {
    val numActiveUsers = config.getInt("number_of_active_users")
    val numBucket = config.getInt("number_of_bucket")
    val backOffInMillis = config.getInt("backoff_in_millis")
    val numRetries = config.getInt("num_retries")
    EsExportConfig(numActiveUsers, numBucket, backOffInMillis, numRetries)
  }
}
