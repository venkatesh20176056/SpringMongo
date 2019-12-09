package com.paytm.map.features.utils.monitoring

import com.codahale.metrics.MetricRegistry
import com.paytm.map.features.utils.Settings

/**
 * A sink to write all metrics
 */
trait FeaturesMonitorSink {
  def register(metricRegistry: MetricRegistry): Unit
}

object FeaturesMonitorSink {
  /**
   * A factory which can create a [[FeaturesMonitorSink]] from a config.
   */
  trait FeaturesMonitorSinkFactory[T <: FeaturesMonitorSink] {
    def fromSettings(settings: Settings): T
  }
}
