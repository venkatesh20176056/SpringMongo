package com.paytm.map.features.config

import com.typesafe.config.Config

case class ESConfig(
  host: String,
  index: String,
  indexType: String,
  port: Int
)

object ESConfig {
  def apply(config: Config): ESConfig = {
    val host: String = config.getString("host")
    val index: String = config.getString("index")
    val indexType: String = config.getString("index_type")
    val port: Int = config.getInt("port")

    ESConfig(
      host,
      index,
      indexType,
      port
    )
  }
}

