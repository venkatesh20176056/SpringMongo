package com.paytm.map.features.config

import com.typesafe.config.Config

case class RedshiftConfig(
  host: String,
  db: String,
  user: String,
  passwordPath: String,
  features: String,
  port: Int = 5439
)

object RedshiftConfig {
  def apply(config: Config): RedshiftConfig = {
    val host: String = config.getString("host")
    val db: String = config.getString("db")
    val user: String = config.getString("user")
    val passwordPath: String = config.getString("password_path")
    val features: String = config.getString("features")

    RedshiftConfig(
      host,
      db,
      user,
      passwordPath,
      features
    )
  }
}
