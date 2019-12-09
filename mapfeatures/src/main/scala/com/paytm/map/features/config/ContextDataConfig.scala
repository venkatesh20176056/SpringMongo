package com.paytm.map.features.config

import com.typesafe.config.Config
case class ContextDataConfig(geohashPrecision: Int)

object ContextDataConfig {
  def apply(config: Config): ContextDataConfig = {

    val geohashPrecision = config.getInt("geo_hash_precision")
    ContextDataConfig(geohashPrecision)
  }
}
