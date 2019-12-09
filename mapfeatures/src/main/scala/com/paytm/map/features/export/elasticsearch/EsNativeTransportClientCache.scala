package com.paytm.map.features.export.elasticsearch

import org.apache.spark.internal.Logging
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.mutable

/**
 * Caches TransportClients per JVM.
 */
object EsNativeTransportClientCache extends Logging {
  private val transportClients = mutable.HashMap.empty[EsNativeTransportClientConf, TransportClient]

  private def buildTransportSettings(clientConf: EsNativeTransportClientConf): Settings = {
    import EsNativeTransportClientConf._

    val temporaryEsSettingsBuilder = Settings.builder()

    clientConf.transportSettings foreach { currentSetting =>
      temporaryEsSettingsBuilder.put(currentSetting._1, currentSetting._2)
    }

    temporaryEsSettingsBuilder.put(CONFIG_CLIENT_TRANSPORT_SNIFF, false)
    temporaryEsSettingsBuilder.build()
  }

  private def buildTransportClient(clientConf: EsNativeTransportClientConf): TransportClient = {
    val esSettings = buildTransportSettings(clientConf)

    val temporaryEsClient = new PreBuiltTransportClient(esSettings)

    clientConf.transportAddresses foreach { inetSocketAddress =>
      temporaryEsClient.addTransportAddresses(new TransportAddress(inetSocketAddress))
    }

    sys.addShutdownHook {
      log.info("Closed Elasticsearch Transport Client.")

      temporaryEsClient.close()
    }

    log.info(s"Connected to the following Elasticsearch nodes: ${temporaryEsClient.connectedNodes()}.")

    temporaryEsClient
  }

  /**
   * Gets or creates a TransportClient per JVM.
   *
   * @param clientConf Settings and initial endpoints for connection.
   * @return TransportClient.
   */
  def getTransportClient(clientConf: EsNativeTransportClientConf): TransportClient = {
    transportClients.get(clientConf) match {
      case Some(transportClient) => transportClient
      case None =>
        val transportClient = buildTransportClient(clientConf)
        transportClients.put(clientConf, transportClient)
        transportClient
    }
  }
}
