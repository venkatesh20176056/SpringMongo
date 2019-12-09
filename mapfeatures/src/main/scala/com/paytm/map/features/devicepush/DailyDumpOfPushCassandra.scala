package com.paytm.map.features.devicepush

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.types.DoubleType

object DailyDumpOfPushCassandra extends DailyDumpOfPushCassandraJob
  with SparkJob with SparkJobBootstrap

trait DailyDumpOfPushCassandraJob {
  this: SparkJob =>

  val JobName = "DailyDumpOfPushCassandra"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.FormatPattern)

    val userTokenConfig = settings.userTokenCassandraCfg
    val deviceInfoConfig = settings.deviceInfoCassandraCfg

    spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option(userTokenConfig.host))

    val connectorToClusterOne = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", userTokenConfig.host))
    implicit val c = connectorToClusterOne

    val user_token: DataFrame = spark.sparkContext.cassandraTable[(Option[String], Option[String], Option[String], Option[String], Option[Boolean], Option[Boolean], Option[Long], Option[Long])](userTokenConfig.keyspace, userTokenConfig.tableName)
      .select("userid", "platform", "pushid", "fcmtoken", "isdisabled", "isloggedout", "fcmtoken".writeTime, "isloggedout".writeTime)
      .toDF("userid", "platform", "pushid", "fcmtoken", "isdisabled", "isloggedout", "fcmtoken_writetime", "isloggedout_writetime")

    user_token
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${settings.featuresDfs.pushCassandraUserToken}/dt=$targetDateStr")

    val deviceInfo = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> deviceInfoConfig.tableName, "keyspace" -> deviceInfoConfig.keyspace, "cluster" -> "Cluster1", "spark.cassandra.connection.local_dc" -> "ap-south"))
      .load()

    val device_info_writeTime: DataFrame = spark.sparkContext.cassandraTable[(Option[String], Option[Long])]("push_registry", "deviceinfo")

      .select(
        "pushid",
        "fcmtoken".writeTime
      )
      .toDF(
        "pushid",
        "fcmtoken_writeTime"
      )

    deviceInfo
      .join(device_info_writeTime, "pushid")
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${settings.featuresDfs.pushCassandraDeviceInfo}/dt=$targetDateStr")
  }

}

