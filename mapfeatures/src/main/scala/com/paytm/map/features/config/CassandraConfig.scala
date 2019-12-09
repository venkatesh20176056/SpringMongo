package com.paytm.map.features.config

import com.typesafe.config.Config

case class CassandraConfig(
  keyspace: String,
  tableName: String,
  host: String
)

object CassandraConfig {

  def apply(config: Config): CassandraConfig =
    new CassandraConfig(
      config.getString("keyspace"),
      config.getString("tableName"),
      config.getString("host")
    )

}