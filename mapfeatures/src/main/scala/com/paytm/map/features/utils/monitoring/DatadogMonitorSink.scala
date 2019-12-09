package com.paytm.map.features.utils.monitoring

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.paytm.map.features.utils.Settings
import com.paytm.map.features.utils.monitoring.FeaturesMonitorSink.FeaturesMonitorSinkFactory
import com.typesafe.config.Config
import org.coursera.metrics.datadog.{DatadogReporter, DefaultMetricNameFormatter}
import org.coursera.metrics.datadog.transport.HttpTransport

class DatadogMonitorSink(apiKey: String, flushIntervalSeconds: Long) extends FeaturesMonitorSink {

  override def register(metricRegistry: MetricRegistry): Unit = {

    val httpTransport = new HttpTransport.Builder().withApiKey(apiKey).build()

    val reporter = DatadogReporter.forRegistry(metricRegistry)
      .withEC2Host()
      .withTransport(httpTransport)
      .withMetricNameFormatter(new DefaultMetricNameFormatter())
      .build()

    reporter.start(flushIntervalSeconds, TimeUnit.SECONDS)
    sys.addShutdownHook {
      reporter.report()
      reporter.stop()
    }

  }
}

object DatadogMonitorSink {
  object DatadogMonitorSinkFactory extends FeaturesMonitorSinkFactory[DatadogMonitorSink] {

    override def fromSettings(settings: Settings): DatadogMonitorSink = {
      val config = settings.rootConfig
      val datadogConfig = config.getConfig("monitoring.datadog")

      val apiKey = settings.getParameter("/common/DATADOG_API_KEY")
      val ddAgentFlushIntervalSeconds = datadogConfig.getInt("flush-interval")
      new DatadogMonitorSink(apiKey, ddAgentFlushIntervalSeconds)
    }
  }
}
