package com.paytm.map.features.utils.athena

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}

object AthenaClientFactory {
  final private val builder: AmazonAthenaClientBuilder = AmazonAthenaClientBuilder.standard
    .withRegion(Regions.AP_SOUTH_1)
    .withClientConfiguration(new ClientConfiguration())

  val createClient: AmazonAthena = builder.build
}
