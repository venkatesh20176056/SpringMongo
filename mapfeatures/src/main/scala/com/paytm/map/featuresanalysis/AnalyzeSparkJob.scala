package com.paytm.map.featuresanalysis

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.codahale.metrics.Gauge
import com.esotericsoftware.minlog.Log
import com.paytm.map.features.utils.Settings
import com.paytm.map.features.utils.monitoring.FeaturesMonitor
import com.qubole.sparklens.common.{AggregateMetrics, AppContext}
import com.qubole.sparklens.helper.JobOverlapHelper
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger
import org.coursera.metrics.datadog.TaggedName.TaggedNameBuilder
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.parse
import com.esotericsoftware.minlog.Log._

import scala.io.Source
import scala.io.Source.fromInputStream

object AnalyzeSparkJob extends App {
  val fileArgs: Array[String] = args(0).replaceFirst("s3[a]?[n]?://", "").split("/")
  val bucket: String = fileArgs(0)
  val env: String = fileArgs(1)
  val jobName: String = fileArgs(2)
  val key: String = fileArgs.tail.mkString("/")
  val rmHost = args(1)
  println(s"[Running spark analysis] env: $env, job: $jobName, bucket: $bucket, key: $key")
  val analyzeJob: AnalyzeSparkJob = new AnalyzeSparkJob(jobName, env, rmHost)
  analyzeJob.startAnalyze(bucket, key)
}

class AnalyzeSparkJob(mapFeatureJob: String, env: String, rmHost: String) {
  val settings = new Settings

  def startAnalyze(bucket: String, key: String): Unit = {
    val s3Client = AmazonS3ClientBuilder.standard().build()
    val s3Object = s3Client.getObject(bucket, key)
    val content = Source.fromInputStream(s3Object.getObjectContent).getLines
    startAnalysersFromString(content.mkString)
  }

  private def startAnalysersFromString(json: String): Unit = {
    implicit val formats = DefaultFormats
    val map = parse(json).extract[JValue]

    val appContext = AppContext.getContext(map)
    analyze(appContext, appContext.appInfo.startTime, appContext.appInfo.endTime)
  }

  case class YarnAppInfo(memoryMBSeconds: Long, vcoreSeconds: Long)

  private def getFromYarnAppInfo(applicationID: String): YarnAppInfo = {
    implicit val formats = DefaultFormats

    val httpClient = HttpClientBuilder.create.build
    val url = s"http://$rmHost:8088/ws/v1/cluster/apps/$applicationID"
    val httpResponse = httpClient.execute(new HttpGet(url))
    val responseCode = httpResponse.getStatusLine.getStatusCode
    if (responseCode == 200) {
      val inputStream = httpResponse.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      val kv = (JsonMethods.parse(content) \ "app").extract[Map[String, JValue]]
      YarnAppInfo(kv("memorySeconds").extract[Long], kv("vcoreSeconds").extract[Long])
    } else {
      val message = s"Error in retrieving application info from resource manager $rmHost:8088. Response code: $responseCode."
      Log.error(message)
      YarnAppInfo(0, -1)
    }
  }

  // copied from spark-lens[com.qubole.sparklens.analyzer.EfficiencyStatisticsAnalyzer.scala]
  private def analyze(appContext: AppContext, startTime: Long, endTime: Long): Unit = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    // wall clock time, appEnd - appStart
    val appTotalTime = endTime - startTime
    // wall clock time per Job. Aggregated
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(ac)
    /* sum of cores in all the executors:
     * There are executors coming up and going down.
     * We are taking the max-number of executors running at any point of time, and
     * multiplying it by num-cores per executor (assuming homogenous cluster)
     */
    val maxExecutors = AppContext.getMaxConcurrent(ac.executorMap, ac)
    val executorCores = AppContext.getExecutorCores(ac)
    val totalCores = executorCores * maxExecutors

    // total compute millis available to the application
    val appComputeMillisAvailable = totalCores * appTotalTime
    val computeMillisFromExecutorLifetime = ac.executorMap.map(x => {
      val ecores = x._2.cores
      val estartTime = Math.max(startTime, x._2.startTime)
      val eendTime = if (x._2.isFinished()) {
        Math.min(endTime, x._2.endTime)
      } else {
        endTime
      }
      ecores * (eendTime - estartTime)
    }).sum

    // some of the compute millis are lost when driver is doing some work
    // and has not assigned any work to the executors
    // We assume executors are only busy when one of the job is in progress
    val inJobComputeMillisAvailable = totalCores * jobTime
    // Minimum time required to run a job even when we have infinite number
    // of executors, essentially the max time taken by any task in the stage.
    // which is in the critical path. Note that some stages can run in parallel
    // we cannot reduce the job time to less than this number.
    // Aggregating over all jobs, to get the lower bound on this time.
    val criticalPathTime = JobOverlapHelper.criticalPathForAllJobs(ac)

    //sum of millis used by all tasks of all jobs
    val inJobComputeMillisUsed = ac.jobMap.values
      .filter(x => x.endTime > 0)
      .filter(x => x.jobMetrics.map.isDefinedAt(AggregateMetrics.executorRuntime))
      .map(x => x.jobMetrics.map(AggregateMetrics.executorRuntime).value)
      .sum

    val perfectJobTime = inJobComputeMillisUsed / totalCores
    val driverTimeJobBased = appTotalTime - jobTime
    val driverComputeMillisWastedJobBased = driverTimeJobBased * totalCores
    val driverWastedPercentOverAll = driverComputeMillisWastedJobBased * 100 / appComputeMillisAvailable.toFloat
    val executorWastedPercentOverAll = (inJobComputeMillisAvailable - inJobComputeMillisUsed) * 100 / appComputeMillisAvailable.toFloat

    val tagMap: Map[String, String] = Map("mapFeatureJob" -> mapFeatureJob, "env" -> env)
    publishMetric("appTotalTime", appTotalTime, tagMap)
    publishMetric("perfectJobTime", driverTimeJobBased + perfectJobTime, tagMap)
    publishMetric("criticalPathTime", driverTimeJobBased + criticalPathTime, tagMap)

    val driverClockTimeValue = driverTimeJobBased * 100 / appTotalTime.toFloat
    publishMetric("driverClockTime", driverClockTimeValue, tagMap)

    val executorClockTimeValue = jobTime * 100 / appTotalTime.toFloat
    publishMetric("executorClockTime", executorClockTimeValue, tagMap)

    val wastedDriverTime = driverWastedPercentOverAll
    publishMetric("wastedDriverTime", wastedDriverTime, tagMap)

    val wastedExecutorTime = executorWastedPercentOverAll
    publishMetric("wastedExecutorTime", wastedExecutorTime, tagMap)

    val peakExecutionMemory = ac.appMetrics.map(AggregateMetrics.peakExecutionMemory).max
    publishMetric("maxPeakExecutionMemory", peakExecutionMemory, tagMap)

    val appInfo: YarnAppInfo = getFromYarnAppInfo(ac.appInfo.applicationID)
    publishMetric("memory_seconds_mb", appInfo.memoryMBSeconds, tagMap)
    publishMetric("vcore_seconds", appInfo.vcoreSeconds, tagMap)
  }

  private def publishMetric(metricType: String, metricValue: Any, tagMap: Map[String, String]): Unit = {
    val levelDataDogMetric = s"midgar.sparkanalysis.$metricType"
    gaugeValuesWithTags(
      metricName  = levelDataDogMetric,
      metricValue = metricValue,
      tagMap      = tagMap
    )
  }

  def gaugeValuesWithTags(metricName: String, tagMap: Map[String, String],
    metricValue: Any): Gauge[Any] = {
    val tagBuilder = new TaggedNameBuilder()

    tagMap.foreach {
      case (tagName, tagValue) =>
        tagBuilder.addTag(s"$tagName:$tagValue")
    }
    val metricNameWithTag = tagBuilder
      .metricName(metricName)
      .build()
      .encode()

    gaugeValues(metricNameWithTag, metricValue)
  }

  def gaugeValues(metricName: String, metricValue: Any): Gauge[Any] = {
    println(metricName, metricValue)
    val monitor = FeaturesMonitor.getJobMonitor("")
    monitor.gauge(metricName, () => metricValue)
  }
}
