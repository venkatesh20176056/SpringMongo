package com.paytm.map.features.config

import com.typesafe.config.Config

case class CampaignDfsConfig(
  betaoutEvents: String,
  sendgridEvents: String
)

object CampaignDfsConfig {

  def apply(config: Config): CampaignDfsConfig = {
    // extracted from config
    val betaoutEvents: String = config.getString("betaout_events")
    val sendgridEvents: String = config.getString("sendgrid_events")

    CampaignDfsConfig(
      betaoutEvents,
      sendgridEvents
    )
  }

}
