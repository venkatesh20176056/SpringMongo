package com.paytm.map.features.utils.monitoring

import com.codahale.metrics._
import com.paytm.map.features.utils.Settings
import com.typesafe.config.Config

import scala.util.Try

object FeaturesMonitor {

  implicit val settings = (new Settings)
  def getJobMonitor(jobName: String): FeaturesMonitor = new FeaturesMonitor(jobName)
}

class FeaturesMonitor(jobName: String)(implicit val settings: Settings) extends Serializable {

  private val config = settings.rootConfig
  private val applicationName = config.getString("application-name")
  private val environment = config.getString("env")

  @transient
  protected lazy val featuresMonitorSink: FeaturesMonitorSink = {
    DatadogMonitorSink.DatadogMonitorSinkFactory.fromSettings(settings)
  }

  @transient
  private lazy val metricRegistry = {
    val tmpRegistry = new MetricRegistry()
    featuresMonitorSink.register(tmpRegistry)
    tmpRegistry
  }

  def gauge[T](name: String, metricFunc: () => T): Gauge[T] = {
    val currentMetricName = formatMetricName(name)
    val tmpGauge = new Gauge[T] {
      override def getValue: T = metricFunc()
    }

    Try(metricRegistry.register(currentMetricName, tmpGauge))
      .getOrElse(metricRegistry.getGauges.get(currentMetricName))
      .asInstanceOf[Gauge[T]]
  }

  def histogram(name: String): Histogram = metricRegistry.histogram(formatMetricName(name))

  def meter(name: String): Meter = metricRegistry.meter(formatMetricName(name))

  def counter(name: String): Counter = metricRegistry.counter(formatMetricName(name))

  def timer(name: String): Timer = metricRegistry.timer(formatMetricName(name))

  private def formatMetricName(name: String): String = MetricRegistry.name(s"${environment}.${applicationName}", name)
}
