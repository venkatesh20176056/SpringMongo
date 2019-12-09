package com.paytm.map.features.utils.monitoring

import com.paytm.map.features.SparkJobBase

trait Monitoring {
  self: SparkJobBase =>

  lazy val monitor = FeaturesMonitor.getJobMonitor(self.JobName)
}
