package com.paytm.map.features.base.contextdata

sealed trait Context

case class RequestGeo(lat: Option[String], long: Option[String]) extends Context

case class DeviceContext(
  deviceType: Option[String],
  aaid: Option[String],
  idfa: Option[String],
  browserUuid: Option[String],
  make: Option[String],
  hwv: Option[String],
  os: Option[String],
  osv: Option[String],
  geo: Option[RequestGeo]
) extends Context

case class RequestContext(
  channel: Option[String],
  version: Option[String],
  device: Option[DeviceContext]
) extends Context

case class GeoTimePoint(
  geohash: String,
  time: Long
) extends Context

case class CategoryTimePoint(
  category: Long,
  time: Long
) extends Context

case class CategoryFrequency(
  category: Long,
  frequency: Long
)

case class GeoRegionCount(
  geoRegion: String,
  count: Long
) extends Context