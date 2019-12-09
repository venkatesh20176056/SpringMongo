package com.paytm.map.features.utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path

object FileUtils {

  def getLatestTablePath(pathPrefix: String, spark: SparkSession, endDate: String, datePrefix: String = "", terminalString: String = "_SUCCESS"): Option[String] = {
    val date = getLatestTableDate(pathPrefix, spark, endDate, datePrefix, terminalString)
    date match {
      case Some(x) => Some(s"$pathPrefix/$datePrefix$x")
      case _       => None
    }
  }

  def getLatestTableDate(pathPrefix: String, spark: SparkSession, endDate: String, datePrefix: String = "", terminalString: String = "_SUCCESS"): Option[String] = {
    getLatestPathListings(pathPrefix, spark, terminalString)
      .map(_.split("/").last)
      .map(_.stripPrefix(datePrefix))
      .filter(_ <= endDate) // Only keep paths before `endDate`
      .sorted
      .lastOption
  }

  def getAvlTableDates(pathPrefix: String, spark: SparkSession, datePrefix: String = ""): Seq[String] = {
    getLatestPathListings(pathPrefix, spark)
      .map(_.split("/").last)
      .map(_.stripPrefix(datePrefix))
      .sorted
  }

  def fs(path: Path, spark: SparkSession) =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)

  def pathExists(path: String, spark: SparkSession): Boolean = {
    pathExists(new Path(path), spark)
  }

  def pathExists(path: Path, spark: SparkSession): Boolean = {
    fs(path, spark).exists(path)
  }

  /**
   *
   * @param pathPrefix expected format is /path/path1 for full path /path/path1/dateString/_SUCCESS
   * @param spark
   * @return Array[paths] that match pathPrefix
   */
  def getLatestPathListings(pathPrefix: String, spark: SparkSession, terminalString: String = "_SUCCESS"): Array[String] = {
    val globPattern = new Path(pathPrefix.stripSuffix("/") + s"/*/$terminalString")
    fs(new Path(pathPrefix), spark).globStatus(globPattern)
      // extract date part without making assumptions on date regex
      .map(_.getPath.toString.split("/").dropRight(1).mkString("/"))
  }

  /**
   *
   * @param pathPrefix expected format is /path/path1 for full path /path/path1/dateString/_SUCCESS
   * @param spark
   * @return dateString
   */
  def getLatestTableDateStringOption(pathPrefix: String, spark: SparkSession, datePrefix: String = ""): Option[String] = {
    getLatestPathListings(pathPrefix, spark)
      // extract date part without making assumptions on date regex
      .map(_.split("/").last)
      .map(_.stripPrefix(datePrefix))
      .sorted
      .lastOption
  }

  def isFileSuccessfullyCreated(spark: SparkSession, path: String): Boolean = FileUtils.pathExists(path + "/_SUCCESS", spark)

}
