package com.paytm.map.features.config

import com.typesafe.config.Config
case class ParameterStore(pathPrefix: String, roleArn: String)
case class AwsConfig(parameterStore: ParameterStore)

object AwsConfig {
  def apply(config: Config): AwsConfig = {

    val parameterStore = config.getConfig("parameter_store")
    val roleArn = parameterStore.getString("roleArn")
    val pathPrefix = parameterStore.getString("pathPrefix")

    AwsConfig(ParameterStore(pathPrefix, roleArn))
  }
}
