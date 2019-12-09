package com.paytm.map.features.config

import com.typesafe.config.Config

case class BankDfsConfig(
  bankFeaturesPath: String,
  bankProfilePath: String,
  bankUpiPath: String,
  bankUpiStatusPath: String
)

object BankDfsConfig {

  def apply(
    config: Config
  ): BankDfsConfig = {

    val basePath = config.getString("bucket")

    new BankDfsConfig(
      bankFeaturesPath  = s"${basePath}/BankFeatures",
      bankProfilePath   = s"${basePath}/Bankprofile",
      bankUpiPath       = s"${basePath}/UPI",
      bankUpiStatusPath = s"${basePath}/UPI_status"
    )
  }

}
