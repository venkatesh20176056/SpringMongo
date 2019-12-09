package com.paytm.map.features

import com.paytm.map.features.utils.Settings
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

trait SparkJobBootstrap {
  self: SparkJobBase =>

  def main(args: Array[String]): Unit = {
    val settings = new Settings

    val logLevel = settings.jobCfg.logLevel
    Logger.getRootLogger.setLevel(Level.toLevel(logLevel))
    val spark = SparkSession
      .builder()
      .appName(JobName)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)

    spark.sparkContext.parallelize(Seq("")).foreachPartition(_ => {
      import org.apache.log4j.{LogManager, Level}
      LogManager.getRootLogger().setLevel(Level.toLevel(logLevel))
    })

    self match {
      case metricJob: SparkMetricJob if args.contains("--check") =>
        val subArgs = args.filter(_ != "--check")
        val paths = metricJob.metricsInputs(spark, settings, subArgs)
        val metricRootPath = metricJob.metricRootOutputPath(settings, subArgs)
        val metricsLevelOutputPaths = metricJob.generateMetrics(spark, metricRootPath, paths)
        metricJob.publishMetrics(spark, metricsLevelOutputPaths)
      case sparkJob: SparkJob =>
        sparkJob.validate(spark, settings, args) match {
          case SparkJobValid =>
            sparkJob.runJob(spark, settings, args)
            spark.sparkContext.stop()
          case SparkJobInvalid(reason) => throw new RuntimeException(reason)
        }
    }
  }

}