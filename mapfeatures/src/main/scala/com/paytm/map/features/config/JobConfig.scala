package com.paytm.map.features.config

import com.typesafe.config.Config

case class JobConfig(logLevel: String)

object JobConfig {
  def apply(config: Config): JobConfig = {
    val logLevel = config.getString("log_level")
    JobConfig(logLevel)
  }
}