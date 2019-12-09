package com.paytm.map.features.config

import com.typesafe.config.Config

case class ShinraDfsConfig(
  basePath: String,
  modelPath: String
)

object ShinraDfsConfig {

  def apply(config: Config): ShinraDfsConfig = {
    val basePath: String = config.getString("shinra-base")
    val modelPath: String = config.getString("shinra-model")

    ShinraDfsConfig(
      basePath,
      modelPath
    )
  }

}