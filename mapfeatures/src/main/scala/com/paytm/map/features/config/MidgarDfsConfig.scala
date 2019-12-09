package com.paytm.map.features.config

import com.typesafe.config.Config

case class MidgarDfsConfig(locationPath: String)

object MidgarDfsConfig {
  def apply(config: Config): MidgarDfsConfig = {
    val locationPath: String = config.getString("workspace_base") + "customer_city_state"
    MidgarDfsConfig(locationPath)
  }
}