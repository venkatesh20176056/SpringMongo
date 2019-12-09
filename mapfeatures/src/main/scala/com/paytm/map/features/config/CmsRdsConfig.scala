package com.paytm.map.features.config

import com.typesafe.config.Config

case class CmaRdsConfig(host: String, user: String, passwordPath: String, port: Int = 3306)

object CmaRdsConfig {
  def apply(config: Config): CmaRdsConfig = {
    val host: String = config.getString("host")
    val user: String = config.getString("user")
    val passwordPath: String = config.getString("password_path")
    CmaRdsConfig(host, user, passwordPath)
  }
}