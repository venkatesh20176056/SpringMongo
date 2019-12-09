package com.paytm.map.features.export.elasticsearch

import java.net.InetSocketAddress

/**
 * Configurations for EsNativeDataFrameWriter's TransportClient.
 *
 * @param transportAddresses The minimum set of hosts to connect to when establishing a client.
 *                           CONFIG_CLIENT_TRANSPORT_SNIFF is enabled by default.
 * @param transportSettings Miscellaneous settings for the TransportClient.
 *                          Empty by default.
 */
case class EsNativeTransportClientConf(
  transportAddresses: Seq[InetSocketAddress],
  transportSettings: Map[String, String] = Map.empty[String, String]
) extends Serializable

object EsNativeTransportClientConf {
  val CONFIG_CLUSTER_NAME = "cluster.name"
  val CONFIG_CLIENT_TRANSPORT_SNIFF = "client.transport.sniff"
  val CONFIG_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME = "client.transport.ignore_cluster_name"
  val CONFIG_CLIENT_TRANSPORT_PING_TIMEOUT = "client.transport.ping_timeout"
  val CONFIG_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL = "client.transport.nodes_sampler_interval"
}