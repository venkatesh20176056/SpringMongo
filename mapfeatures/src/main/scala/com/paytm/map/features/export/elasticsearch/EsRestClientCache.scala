package com.paytm.map.features.export.elasticsearch

import org.apache.http.HttpHost
import org.apache.spark.internal.Logging
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.mutable

/**
 * Caches TransportClients per JVM.
 */
object EsRestClientCache extends Logging {
  private val transportClients = mutable.HashMap.empty[EsRestClientConf, RestHighLevelClient]

  private def buildTransportSettings(clientConf: EsRestClientConf) = {
    RestClient.builder(clientConf.httpHosts: _*)
  }

  private def buildTransportClient(clientConf: EsRestClientConf): RestHighLevelClient = {
    val esSettings = buildTransportSettings(clientConf)

    val temporaryEsClient = new RestHighLevelClient(esSettings)

    sys.addShutdownHook {
      log.info("Closed Elasticsearch Transport Client.")

      temporaryEsClient.close()
    }

    temporaryEsClient
  }

  /**
   * Gets or creates a TransportClient per JVM.
   *
   * @param clientConf Settings and initial endpoints for connection.
   * @return TransportClient.
   */
  def getTransportClient(clientConf: EsRestClientConf): RestHighLevelClient = {
    transportClients.get(clientConf) match {
      case Some(transportClient) => transportClient
      case None =>
        val transportClient = buildTransportClient(clientConf)
        transportClients.put(clientConf, transportClient)
        transportClient
    }
  }
}
