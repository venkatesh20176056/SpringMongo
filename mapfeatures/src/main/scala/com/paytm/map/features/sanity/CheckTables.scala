package com.paytm.map.features.sanity

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.config.DatalakeDfsConfig.getDataSetIds
import com.paytm.map.features.config.IndexSchema
import com.paytm.map.features.utils.FileUtils.getLatestTableDateStringOption
import com.paytm.map.features.utils.UDFs.gaugeValuesWithTags
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, LocalDateTime, Minutes}

object TableState {
  val inactive = "INACTIVE"
  val notIngested = "NOT-INGESTED"
  val sloViolated = "SLO VIOLATED"
  val allOkay = "ALL OKAY"
}

object CheckTables extends CheckTablesJob with SparkJob with SparkJobBootstrap

trait CheckTablesJob {
  this: SparkJob =>

  val JobName = "CheckTables"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    // Inputs
    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val config = settings.rootConfig.getConfig("dfs_datalake")
    val snapshotEndpoint = config.getString("snapshotQueryEndPointV3")
    val datasetsIds: List[IndexSchema] = getDataSetIds(snapshotEndpoint)

    val metricName = "DataAvailability.status"

    /** *************************** Check V3 ****************************/
    tables
      .filter(_.version == 3)
      .foreach { table =>
        println(table.name)

        val thisTable = datasetsIds.filter(_.name.toLowerCase == s"${table.db}.${table.name}").head

        val (state, gaugeValue) = if (thisTable.status == "INACTIVE") (TableState.inactive, 1)
        else if (thisTable.last_ingest_time == "null") (TableState.notIngested, 1)
        else {
          val slo = thisTable.slo_minutes.toInt
          val FormatPattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ" //"2018-06-06T13:49:43.000+05:30",
          val formatter = DateTimeFormat.forPattern(FormatPattern)
          val ingestTime = formatter.parseDateTime(thisTable.last_ingest_time)
          val currTime = new LocalDateTime().toDateTime(DateTimeZone.forID("Asia/Kolkata"))
          val timeDiff = Minutes.minutesBetween(ingestTime, currTime).getMinutes - slo
          if (timeDiff > 0) (TableState.sloViolated, timeDiff)
          else (TableState.allOkay, 0)
        }

        gaugeValuesWithTags(
          metricName,
          Map("table" -> (table.name + "_v3"), "state" -> state),
          gaugeValue
        )
      }

    /** *************************** Check V2 ****************************/
    tables
      .filter(_.version == 2)
      .foreach { table =>
        println(table.name)

        val latestDate = getTableLatestDate(spark, table.db, table.name, table.partnCol)

        val (state, gaugeValue) = if (latestDate.isEmpty) (TableState.notIngested, 1)
        else if (latestDate.max < targetDateStr) (TableState.sloViolated, 1)
        else (TableState.allOkay, 0)

        gaugeValuesWithTags(
          metricName,
          Map("table" -> (table.name + "_v2"), "state" -> state),
          gaugeValue
        )
      }
  }

  def getTableLatestDate(spark: SparkSession, dbName: String, tableName: String, partnCol: String = "dl_last_updated"): Option[String] = {

    import spark.implicits._
    val tablePath = {
      //getLatestSnapshot
      val bucketName: String = s"daas-datalake-midgar-dropoff"
      val snapshot = spark.read.json(s"s3a://$bucketName/.meta/$dbName.json").map { case Row(a: String) => a }.collect().head
      dbName match {
        case "dwh" => s"s3a://$bucketName/$dbName/hive/.snapshot/$snapshot/$dbName.db/$tableName/"
        case _     => s"s3a://$bucketName/apps/hive/warehouse/$dbName.db/.snapshot/$snapshot/$tableName/"
      }
    }
    println(tablePath)
    getLatestTableDateStringOption(tablePath, spark, s"$partnCol=")
  }

  case class Table(db: String, name: String, partnCol: String, version: Int)

  // TODO : Add tables for SLO check here.
  val tables = Seq(
    Table("dwh", "alipay_pg_olap", "ingest_date", 3),
    Table("dwh", "customer_registration_snapshot", "dl_last_updated", 3),
    Table("kyc", "kyc_user_detail", "dl_last_updated", 3),
    Table("marketplace", "sales_order", "dl_last_updated", 3),
    Table("marketplace", "sales_order_item", "dl_last_updated", 3),
    Table("marketplace", "sales_order_payment", "dl_last_updated", 3),
    Table("code", "promocode_usage", "dl_last_updated", 3),
    Table("marketplace", "catalog_category", "dl_last_updated", 3),
    Table("marketplace", "catalog_product", "dl_last_updated", 3),
    Table("marketplace", "sales_order_metadata", "dl_last_updated", 3),
    Table("fs_recharge", "fulfillment_recharge", "dl_last_updated", 3),
    Table("wallet", "new_system_txn_request", "dl_last_updated", 3),
    Table("wallet", "merchant", "dl_last_updated", 3),
    Table("wallet", "user", "dl_last_updated", 3),
    Table("pg", "entity_demographics", "dl_last_updated", 3),
    Table("pg", "entity_info", "dl_last_updated", 3),
    Table("pg", "uid_eid_mapper", "dl_last_updated", 3),
    Table("pg", "entity_preferences_info", "dl_last_updated", 3),
    Table("digital_reminder", "bills_adanigas", "dl_last_updated", 3),
    Table("digital_reminder", "bills_aircel", "dl_last_updated", 3),
    Table("digital_reminder", "bills_airtel", "dl_last_updated", 3),
    Table("digital_reminder", "bills_ajmerelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_apepdcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_assampower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bangalorewater", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bescom", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bescom_ebps", "dl_last_updated", 3),
    Table("digital_reminder", "bills_best", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bhagalpurelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bharatpurelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bses", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bses_bkup", "dl_last_updated", 3),
    Table("digital_reminder", "bills_bsnl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_calcuttaelectric", "dl_last_updated", 3),
    Table("digital_reminder", "bills_cspdcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_delhijalboard", "dl_last_updated", 3),
    Table("digital_reminder", "bills_dishtv", "dl_last_updated", 3),
    Table("digital_reminder", "bills_dps", "dl_last_updated", 3),
    Table("digital_reminder", "bills_essel", "dl_last_updated", 3),
    Table("digital_reminder", "bills_gescom", "dl_last_updated", 3),
    Table("digital_reminder", "bills_haryanaelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_hescom", "dl_last_updated", 3),
    Table("digital_reminder", "bills_iciciinsurance", "dl_last_updated", 3),
    Table("digital_reminder", "bills_idea", "dl_last_updated", 3),
    Table("digital_reminder", "bills_igl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_indiafirst", "dl_last_updated", 3),
    Table("digital_reminder", "bills_indiapower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_jaipurelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_jodhpurelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_jusco", "dl_last_updated", 3),
    Table("digital_reminder", "bills_kotaelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_kseb", "dl_last_updated", 3),
    Table("digital_reminder", "bills_mahanagargas", "dl_last_updated", 3),
    Table("digital_reminder", "bills_manappuram", "dl_last_updated", 3),
    Table("digital_reminder", "bills_mangaloreelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_matrixpostpaid", "dl_last_updated", 3),
    Table("digital_reminder", "bills_meptolls", "dl_last_updated", 3),
    Table("digital_reminder", "bills_mpelectricity", "dl_last_updated", 3),
    Table("digital_reminder", "bills_msedcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_mumbaimetro", "dl_last_updated", 3),
    Table("digital_reminder", "bills_noidapower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_northbiharpower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_orrissadiscoms", "dl_last_updated", 3),
    Table("digital_reminder", "bills_pspcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_reliance", "dl_last_updated", 3),
    Table("digital_reminder", "bills_relianceenergy", "dl_last_updated", 3),
    Table("digital_reminder", "bills_relianceinsurance", "dl_last_updated", 3),
    Table("digital_reminder", "bills_reliancemobile", "dl_last_updated", 3),
    Table("digital_reminder", "bills_sitienergy", "dl_last_updated", 3),
    Table("digital_reminder", "bills_southbiharpower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_t24", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tata_ebps", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tatadocomo", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tataindicom", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tatalandline", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tataphoton", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tatapower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tatapowermumbai", "dl_last_updated", 3),
    Table("digital_reminder", "bills_telanganapower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_torrentpower", "dl_last_updated", 3),
    Table("digital_reminder", "bills_tsnpdcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_uppcl", "dl_last_updated", 3),
    Table("digital_reminder", "bills_vodafone", "dl_last_updated", 3),
    Table("digital_reminder", "bills_wbsedcl", "dl_last_updated", 3),
    Table("sms_data", "user_bills", "dl_last_updated", 3),
    Table("sms_data", "purchases", "dl_last_updated", 3),
    Table("sms_data", "travel", "dl_last_updated", 3)
  )
}