package com.paytm.map.features.export.elasticsearch

import java.net.InetSocketAddress

import org.apache.http.HttpHost

/**
 * Configurations for EsNativeDataFrameWriter's TransportClient.
 *
 * @param httpHosts The minimum set of hosts to connect to when establishing a client.
 * @param transportSettings Miscellaneous settings for the TransportClient.
 *                          Empty by default.
 */
case class EsRestClientConf(
  httpHosts: Seq[HttpHost],
  transportSettings: Map[String, String] = Map.empty[String, String],
  numBuckets: Int = 10
) extends Serializable

