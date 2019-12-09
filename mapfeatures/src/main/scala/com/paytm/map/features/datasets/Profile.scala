package com.paytm.map.features.datasets

import java.sql.Timestamp

import com.paytm.map.features.SparkJobValidations._
import com.paytm.map.features.base.DataTables
import com.paytm.map.features.campaigns.CampaignConstants._
import com.paytm.map.features.datasets.Constants._
import com.paytm.map.features.utils.ConvenientFrame.{configureSparkForMumbaiS3Access, _}
import com.paytm.map.features.utils.FileUtils.getLatestTableDate
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType, _}
import org.joda.time.DateTime

object Profile extends ProfileJob with SparkJob with SparkJobBootstrap
case class CustomerRecord(alipay_user_id: String, email_id: String, phone_no: String,
  sign_up_date: Timestamp, is_email_verified: String, gender: String,
  age: java.lang.Integer, update_timestamp: Timestamp, first_name: String, full_name: String)
case class PayAliMappingRecord(paytm_user_id: String, alipay_user_id: String, update_time: Timestamp)
case class DeviceOS(osName: String, osVersion: String)

trait ProfileJob {
  this: SparkJob =>

  val JobName = "Profile"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def convertCase(col: Column) = {
    when(col === "null" || col === "NULL" || trim(col) === "", null)
      .otherwise(
        initcap(col)
      )
  }

  def concatColumns(col1: Column, col2: Column) = {
    when(col1.isNull && col2.isNotNull, col2)
      .otherwise(
        when(col1.isNotNull && col2.isNull, col1)
          .otherwise(
            concat(col1, lit(" "), col2)
          )
      )
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import settings._
    import spark.implicits._

    /** args dates **/
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr: String = targetDate.toString(ArgsUtils.formatter)
    val targetDateMinusOneStr = targetDate.minusDays(1).toString(ArgsUtils.formatter)
    val targetYear: Int = targetDate.getYear

    /** List of input non-datalake paths used in the job **/
    // Shinra Paths
    val locationPath = s"${shinraDfs.basePath}/customer_city_state/latest"
    val shinraTopCatsPath = s"${shinraDfs.modelPath}/neuralEmbeddingCCOutputs/customSegments/T2ActivationSegments/topUsersPerCat/".replace("stg", "prod")
    val ccMixerOutputPath = s"${shinraDfs.modelPath}/mixer/customSegments/T2ActivationSegments/topUserCats/"
    // Feature paths
    val salesDataPath = settings.featuresDfs.baseDFS.salesDataPath
    val auditLogsTablePath = s"${featuresDfs.baseDFS.auditLogsTablePath}"
    val deviceOSPath = s"${featuresDfs.baseDFS.aggPath}/DeviceOs/"
    val supListPath = featuresDfs.suppressionTable
    val campaignsPath = s"${featuresDfs.campaignsTable}/country=india/"
    val merchantDataPath = featuresDfs.baseDFS.merchantsPath
    val upiStatusPath = s"${featuresDfs.baseDFS.aggPath}/UPI_status"
    val goldBalancePath = s"${featuresDfs.baseDFS.aggPath}/GoldWallet/dt=$targetDateStr"

    /** output path **/
    val profilePath = s"${featuresDfs.featuresTable}/${featuresDfs.profile}dt=$targetDateStr"

    /** Location features **/
    val location = spark.read.parquet(locationPath).
      withColumn("city", when($"canonical_city".isNotNull, standardizeCity($"canonical_city")).otherwise(null)).
      withColumn("state", when($"state".isNotNull, standardizeState($"state")).otherwise(null)).
      withColumn("country", when($"country".isNotNull, standardizeCountry($"country")).otherwise(null)).
      withColumn("pin_code", when($"pin_code".isNotNull, standarizePIN($"pin_code")).otherwise(null)).
      select($"customer_id", $"city", $"state", $"country", $"pin_code")

    /** Read shinra cc activation segment **/
    val shinraTopCatsLatestDate = spark.read.parquet(shinraTopCatsPath).select($"date".cast(StringType))
      .agg(max("date")).head.getAs[String](0)

    val shinraTopCats = spark.read.parquet(shinraTopCatsPath)
      .filter($"date" === shinraTopCatsLatestDate)
      .select($"customer_id", $"l2-pseudo-id" as "l2_pseudo_id")
      .groupBy("customer_id").agg(collect_list("l2_pseudo_id") as "l2_pseudo_id")

    /** Read top categories per customer as predicted by cc mixer **/
    val ccMixerLatestDate = spark.read.parquet(ccMixerOutputPath).select($"date".cast(StringType))
      .agg(max("date")).head.getAs[String](0)

    val ccMixerTopCats = spark.read.parquet(ccMixerOutputPath)
      .filter($"date" === ccMixerLatestDate)
      .select($"customer_id", $"category")
      .groupBy("customer_id").agg(collect_list("category") as "paytm_l2_pseudo_id")

    /** DEVICE_OS feature (moved from wallet to profile) **/
    val getOS = udf((channel_id: String) => {
      channel_id match {
        case s if channel_id.contains("ANDROID") => "ANDROID"
        case s if channel_id.contains("WINDOWS") => "WINDOWS"
        case s if channel_id.contains("IOSAPP") => "IOS"
        case s if channel_id.contains("IPAD") => "IOS"
        case s if channel_id.contains("MALLAPP") => "ANDROID"
        case s if channel_id.contains("PAYTMSECUREBUSINESS") => "ANDROID"
        case s if channel_id.contains("HTML") => "WEB"
        case s if channel_id.contains("WEB") => "WEB"
        case s if channel_id.startsWith("APP ") => "WEB"
        case s if channel_id.contains("WAP") => "WEB"
        case _ => "OTHER"
      }
    })

    val salesTable = spark.read.parquet(salesDataPath)
      .where($"dt".between(targetDate.minusDays(90).toString(ArgsUtils.formatter), targetDateStr))
      .where($"successfulTxnFlag" === 1)
    val deviceOS = salesTable
      .where($"channel_id".isNotNull)
      .select("customer_id", "channel_id", "dt")
      .withColumn("platform", getOS(col("channel_id")))
      .groupBy("customer_id")
      .agg(collect_set("platform").as("DEVICE_OS"))

    val contextSchema = StructType(
      StructField("customer_id", LongType, nullable = true) ::
        StructField("device_os_with_version", ArrayType(Encoders.product[DeviceOS].schema), nullable = true) ::
        StructField("app_version", ArrayType(StringType)) ::
        Nil
    )

    /** Get Device OS Info **/
    // Read today's audit log table and get device info
    // If data is not available, create empty dataframe
    val deviceOSTodayData = try {
      val deviceOSPath = s"$auditLogsTablePath/dt=$targetDateStr"
      spark.read.parquet(deviceOSPath)
        .filter($"customer_id" > 0)
        .withColumn("device_os", when($"deviceType" === "PHONE", lower($"os")).otherwise(when($"deviceType" === "PC", lit("web")).otherwise(null)))
        .withColumn("standardized_os_version", when($"osv".isNotNull, standarizeVersion($"osv")).otherwise("unknown"))
        .withColumn("device_os_with_version", toDeviceOS($"device_os", $"standardized_os_version"))
        .withColumn("app_version", when($"version".isNotNull, standarizeVersion($"version")).otherwise("unknown"))
        .groupBy("customer_id")
        .agg(collect_set("device_os_with_version") as "device_os_with_version", collect_set("app_version") as "app_version")
    } catch {
      case e: Throwable =>
        log.warn("No device OS data available for today")

        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], contextSchema)
    }

    // Read latest accumulated audit log table and union with today
    val latestAvailablePath = getLatestTableDate(deviceOSPath, spark, targetDateMinusOneStr, "dt=")
    val deviceOSAggregatedData = latestAvailablePath match {
      case Some(latestPath) => spark.read.parquet(s"$deviceOSPath/dt=$latestPath")
      case None             => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], contextSchema)
    }

    val deviceOSData = deviceOSAggregatedData.union(deviceOSTodayData)
      .groupBy("customer_id")
      .agg(
        collect_set("device_os_with_version") as "device_os_with_version",
        collect_set("app_version") as "app_version"
      )
      .withColumn("device_os_with_version", flattenDeviceOS($"device_os_with_version"))
      .withColumn("app_version", flattenVersion($"app_version"))

    // Save new accumulated data in today's path
    deviceOSData.write.mode(SaveMode.Overwrite).parquet(s"$deviceOSPath/dt=$targetDateStr")

    val deviceOSDataAndLatestAppVersion = deviceOSData
      .select(
        $"customer_id",
        $"device_os_with_version",
        $"app_version",
        when($"app_version".isNotNull, array_max($"app_version")) as "latest_app_version"
      )

    /** reading mumbai **/

    configureSparkForMumbaiS3Access(spark)

    /** Feature to flag customers who can receieve emails **/
    val supList = spark.read.option("header", "true").
      csv(supListPath).
      where($"email".isNotNull && (trim($"email") =!= "")).
      select($"email" as "email_id").
      na.drop.
      distinct.
      withColumn("Receive_email", lit(0))

    /** Features from oauth **/
    //If duplicate customer_id exists, keep the one with latest update_timestamp
    val w = Window.partitionBy("alipay_user_id").orderBy($"update_timestamp".desc)
    val custReg = readTableV3(spark, datalakeDfs.dwhCustRegnPath, targetDateStr)
      .withColumn("date_of_birth", unix_timestamp($"date_of_birth", "yyyyMMdd").cast("timestamp")) //change from yyyyMMdd format to timestamp
      .withColumn("tmp", year($"date_of_birth"))
      .withColumn("age", when($"tmp".isNull, null).otherwise(lit(targetYear) - $"tmp").cast("integer"))
      .withColumn("gender", when($"gender".isNotNull, standardizeGender($"gender")).otherwise(null))
      .withColumn("first_name", convertCase($"first_name"))
      .withColumn("last_name", convertCase($"last_name"))
      .withColumn("full_name", concatColumns($"first_name", $"last_name"))
      .select(
        $"customer_registrationid" as "alipay_user_id",
        $"customer_email" as "email_id",
        $"customer_phone".cast(StringType) as "phone_no",
        $"customer_date_of_creation".cast(TimestampType) as "sign_up_date",
        $"email_isverified" as "is_email_verified",
        $"gender", $"age", $"update_timestamp", $"first_name", $"full_name"
      ).as[CustomerRecord]
      .select(
        first("alipay_user_id").over(w) as "alipay_user_id",
        first("email_id").over(w) as "email_id",
        first("phone_no").over(w) as "phone_no",
        first("sign_up_date").over(w) as "sign_up_date",
        first("is_email_verified").over(w) as "is_email_verified",
        first("gender").over(w) as "gender",
        first("age").over(w) as "age",
        first("first_name").over(w) as "first_name",
        first("full_name").over(w) as "full_name"
      )

    //If duplicate mappings exists, keep the one with latest update_time
    val paytmAPlusMapping = getPaytmAPlusMapping(spark, datalakeDfs.paytmAPlusMapping, targetDateStr)

    val cleanCustReg = paytmAPlusMapping
      .join(custReg, Seq("alipay_user_id"), "left_outer")
      .withColumn("customer_id_last_two_digits", pmod($"customer_id", lit(100)))

    /** Feature to flag KYC customers **/
    val kyc = readTableV3(spark, datalakeDfs.kycUserDetailPath, targetDateStr)
      .filter($"account_suspended" === 0)
      .filter(lower($"verification_status") === "verified")
      .select(
        $"custid" as "customer_id",
        lit(1) as "KYC_completion",
        $"usercreation_timestamp".as("BANK_customer_KYC_date")
      )
      .distinct()

    /** Feature for wallet balance and wallet type **/
    val walletFeats = DataTables.walletUser(spark, datalakeDfs.walletUser)
      .join(DataTables.walletDetails(spark, datalakeDfs.walletDetails), Seq("wallet_id"), "inner")
      .groupBy("customer_id")
      .agg(
        sum("balance").as("wallet_balance"),
        first("wallet_type").as("wallet_type"),
        first("trust_factor").as("BANK_trust_factor")
      )

    /** Feature for language **/
    val userAttributeProfile = readTableV3(spark, datalakeDfs.userAttributesSnapshot)
    import org.apache.spark.sql.expressions.Window

    //dedupe customer_registrationid
    val customerWindow = Window.partitionBy('customer_registrationid).orderBy('updated_timestamp.desc)
    val latestLangCol = row_number().over(customerWindow)

    val langProfile: DataFrame = userAttributeProfile
      .filter(upper('profile_key) === "LANGUAGE")
      .withColumn("rank", latestLangCol)
      .filter('rank === 1)
      .select(
        'customer_registrationid as "alipay_user_id",
        standardizeLanguage('profile_doc) as "latest_lang"
      )

    /** Feature from Device signals **/
    val deviceFeaturesDF = spark.read.parquet(s"${featuresDfs.baseDFS.aggPath}/DeviceFeatures")
    val appCategories = spark.read.option("header", "true").option("header", "true").csv(s"${featuresDfs.resources}/app_categories/")
      .withColumn("category", lower($"category"))

    val installedAppsColName = "installed_apps_list"
    val appCategoryCountColName = "app_category_count"
    val preferredCategoryColName = "preferred_app_category"
    val installedCatetoryColName = "installed_apps_category_list"

    val installedAppsCol = collect_set($"app") as installedAppsColName
    val countAppCategories = toAppCategoryCount(collect_list("category")).as(appCategoryCountColName)
    val installAppCatetoryCol = collect_set($"category") as installedCatetoryColName

    //CATEGORIES
    val minUsers = 5000
    val daysBack = 30

    /** apps with more than 5k users in last 30 days **/
    val topApps = {
      val startDateStr = targetDate.minusDays(daysBack).toString(ArgsUtils.formatter)
      val targetDateStr = targetDate.toString(ArgsUtils.formatter)
      deviceFeaturesDF
        .where($"dt".between(startDateStr, targetDateStr))
        .select($"customer_id", explode($"installed_apps_list") as "app")
        .groupBy("app")
        .agg(countDistinct($"customer_id") as "count")
        .where($"count" > minUsers)
        .select("app")
    }.join(appCategories, Seq("app"), "left_outer")

    /** Get app list for most recent dt by customer */
    val installedAppsList = deviceFeaturesDF
      .getFirstLast(Seq("customer_id"), "dt", Seq(s"$installedAppsColName as $installedAppsColName"), false)
      .select(
        explode(col(installedAppsColName)) as "app",
        $"customer_id"
      )
      .join(broadcast(topApps), Seq("app"))
      .groupBy($"customer_id")
      .agg(
        installedAppsCol,
        installAppCatetoryCol,
        countAppCategories as "tmp"
      ).select($"customer_id", col(installedAppsColName), $"tmp._2" as appCategoryCountColName, col(installedCatetoryColName), $"tmp._1" as preferredCategoryColName)

    /** Feature for Bank Profile **/
    val BankProfile = DataTables.TBAADMGeneralAcctMast(spark, settings.datalakeDfs.TBAADMGeneralAcctMast)
      .groupBy("customer_id")
      .agg(
        first($"schm_code").as("BANK_scheme_code"),
        first($"acct_cls_flg").as("BANK_account_closed"),
        first($"del_flg").as("BANK_account_deleted"),
        first($"entity_cre_flg").as("BANK_customer_entity_created"),
        first($"acct_ownership").as("BANK_customer_account_ownership"),
        sum(when(($"del_flg" === "N") && ($"acct_cls_flg" === "N") && ($"entity_cre_flg" === "Y"), 1)
          .otherwise(0)).as("BANK_has_PPB_account"),
        first(when(($"del_flg" === "N") && ($"acct_cls_flg" === "N") && ($"entity_cre_flg" === "Y"), "Y")
          .otherwise("N")).as("BANK_customer_savings_acount_exists"),
        first($"acct_opn_date").as("BANK_acount_opening_date")
      )

    /*
      Kyc Features
       */

    val kycCustomerOlapRaw = readTableV3(spark, settings.datalakeDfs.customerOlap)

    val kycTopCities = kycCustomerOlapRaw.groupBy($"corr_city").count().orderBy(desc("count")).limit(5000).drop("count")
    val kycTopStates = kycCustomerOlapRaw.groupBy($"corr_state").count().orderBy(desc("count")).limit(5000).drop("count")

    val kycCustomerOlapCities = kycCustomerOlapRaw.select($"cust_id".alias("customer_id"), $"corr_city").join(kycTopCities, Seq("corr_city"))
      .withColumnRenamed("corr_city", "kyc_city")
      .withColumn("kyc_city", when($"kyc_city".isNotNull and $"kyc_city" =!= "no data", standardizeCity($"kyc_city")).otherwise("null"))

    val kycCustomerOlapStates = kycCustomerOlapRaw.select($"cust_id".alias("customer_id"), $"corr_state").join(kycTopStates, Seq("corr_state"))
      .withColumnRenamed("corr_state", "kyc_state")
      .withColumn("kyc_state", when($"kyc_state".isNotNull and $"kyc_state" =!= "no data", standardizeState($"kyc_state")).otherwise("null"))

    val kycUserProfile = readTableV3(spark, settings.datalakeDfs.kycUserDetailPath)
      .select($"custid".alias("customer_id"), $"gender")

    val correctedGender = Seq(("b8dc01d1df03939a12d56f90aba98ce4d3226efc3bf20ac51a7a83fe817f6101", "female"), ("ce6297bf33b619de8d1519902f5881549a26558bbb87059d956148fe5d1ce0f1", "male"))
      .toDF("gender", "kyc_gender")

    val kycGender = kycUserProfile.join(broadcast(correctedGender), Seq("gender")).drop("gender")

    /** Debit Card Profile Features **/
    /* TODO: Uncomment once access to bank-product is granted
    val IDCDetails = DataTables.IDCDetails(spark, settings.datalakeDfs.IDCDetails)
      .select("customer_id")
      .distinct()
      .withColumn("tag", lit(1))

    val DebitCardProfile =
      DataTables.physicalDebiCard(spark, settings.datalakeDfs.physicalDebitCard)
        .groupBy("customer_id", "dt")
        .agg(
          min($"status").as("BANK_debit_card_activation_status"),
          min(when($"status" === "ACTIVE", $"updated_at").otherwise(null)).as("BANK_debit_card_activated_date")
        )
        .join(IDCDetails, Seq("customer_id"), "outer")
        .withColumn("BANK_debit_card_form_factor", when($"tag".isNotNull, "INSTA").otherwise("REGULAR"))
     */

    /**
     * Features for betaout and sendgrid campaigns showing most recent opens / sent and number of emails
     * opened in the past 6 months
     */
    val campaignDays = indiaCampaignMonths * 30
    val campaignFilters = getCampaignFilters(campaignDays, targetDateStr)
    val campaignCountAggs = getCampaignCountAggs(indiaCampaignMonths)
    val allCampaigns = spark.read.parquet(campaignsPath)

    val campaignMaxSummary = allCampaigns
      .filterGroupByAggregate(campaignFilters, campaignGroupBy, campaignMaxAggs)
      .where($"email_id".isNotNull && (trim($"email_id") =!= ""))

    val campaignCountSummary = allCampaigns
      .withColumn("campaign_id", regexp_replace($"openedCampaign", "paytm-india_campaign_", ""))
      .filterGroupByAggregate(campaignFilters, campaignGroupBy, campaignCountAggs)
      .where($"email_id".isNotNull && (trim($"email_id") =!= ""))

    /** Feature to count num of unreads since last read **/
    val sentWithReadFlag = allCampaigns
      .select("email_id", "sentTime", "openedTime")
      .filter($"sentTime".isNotNull)
      .withColumn("isRead", when($"openedTime".isNotNull, 1).otherwise(0))
      .drop("openedTime")

    val lastRead = sentWithReadFlag
      .filterGroupByAggregate(
        Seq("isRead == 1"),
        Seq("email_id"),
        Seq(("max", "sentTime as lastRead"))
      )

    val numOfUnreads = sentWithReadFlag.join(lastRead, Seq("email_id"), "left_outer")
      .withColumn("flag", when($"sentTime" > $"lastRead", 1).otherwise(0))
      .withColumn("flag", when($"flag".isNull, 1).otherwise($"flag")) // would be null when customer haven't read any email. In that case all the unreads should be counted, therefore 1.
      .groupByAggregate(
        Seq("email_id"),
        Seq(("sum", "flag as num_of_unreads_since_last_read"))
      )
      .withColumn("num_of_unreads_since_last_read", $"num_of_unreads_since_last_read".cast(IntegerType))
      .where($"email_id".isNotNull && (trim($"email_id") =!= ""))

    /** Merchant Profile Features **/
    // TODO: Deal with same customer multiple merchant scenario
    val merchantProfileFeatures = spark.read.parquet(merchantDataPath)
      .where($"customer_id".isNotNull)
      .select(
        $"customer_id",
        $"mid",
        $"isMerchant",
        $"merchant_first_name",
        $"merchant_last_name",
        $"merchant_email_ID",
        $"merchant_L1_type",
        $"merchant_L2_type",
        $"merchant_category",
        $"merchant_subcategory"
      )
      .dropDuplicates(Seq("customer_id"))

    /** UPI status features **/
    val upiStatus = spark.read.parquet(upiStatusPath)
      .where($"customer_id".isNotNull)

    /** Gold Balance **/
    val goldBalance = spark.read.parquet(goldBalancePath)

    val emailFeatures = cleanCustReg.select($"email_id", $"customer_id")
      .where($"email_id".isNotNull && trim($"email_id") =!= "")
      .join(supList, Seq("email_id"), "left_outer")
      .join(campaignMaxSummary, Seq("email_id"), "left_outer")
      .join(campaignCountSummary, Seq("email_id"), "left_outer")
      .join(numOfUnreads, Seq("email_id"), "left_outer")
      .drop("email_id")

    /** Paytm First Features **/
    val paytmFirstFeatures = readTableV3(spark, settings.datalakeDfs.paytmPrime)
      .select(
        $"customer_id",
        $"date_of_joining" as "last_subscription_date",
        $"plan_expiry_date" as "subscription_expiry_date",
        when($"status" === 1, "YES").otherwise("NO") as "status"
      )
      .addPrefixToColumns("PAYTMFIRST_", Seq("customer_id"))

    /** Join all Features together **/
    val profile: DataFrame = cleanCustReg
      .join(emailFeatures, Seq("customer_id"), "left_outer")
      .join(platformSpecificSignupDateFeature(settings)(spark), Seq("customer_id"), "left_outer")
      .join(shinraTopCats, Seq("customer_id"), "left_outer")
      .join(ccMixerTopCats, Seq("customer_id"), "left_outer")
      .join(kyc, Seq("customer_id"), "left_outer")
      .join(location, Seq("customer_id"), "left_outer")
      .join(walletFeats, Seq("customer_id"), "left_outer")
      .join(merchantProfileFeatures, Seq("customer_id"), "left_outer")
      .join(deviceOS, Seq("customer_id"), "left_outer")
      .join(deviceOSDataAndLatestAppVersion, Seq("customer_id"), "left_outer")
      .join(BankProfile, Seq("customer_id"), "left_outer")
      .join(upiStatus, Seq("customer_id"), "left_outer")
      //.join(DebitCardProfile, Seq("Customer_id"), "left_outer")
      .join(installedAppsList, Seq("customer_id"), "left_outer")
      .join(goldBalance, Seq("customer_id"), "left_outer")
      .join(paytmFirstFeatures, Seq("customer_id"), "left_outer")
      .join(langProfile, Seq("alipay_user_id"), "left_outer")
      .join(kycGender, Seq("customer_id"), "left_outer")
      .join(kycCustomerOlapCities, Seq("customer_id"), "left_outer")
      .join(kycCustomerOlapStates, Seq("customer_id"), "left_outer")
      .join(broadcast(ticketnewUserPhone(spark, settings)), substring_index($"phone_no", "-", -1) === $"user_phone", "left_outer")
      .join(broadcast(ticketnewUserEmail(spark, settings)), $"email_id" === $"user_email", "left_outer")
      .join(broadcast(insiderUserPhone(spark, settings)), substring_index($"phone_no", "-", -1) === $"delivery_tel", "left_outer")
      .join(broadcast(insiderUserEmail(spark, settings)), $"email_id" === $"delivery_email", "left_outer")
      .withColumn("ticketnew_user_flag", coalesce($"ticketnew_user_flag_phone", $"ticketnew_user_flag_email"))
      .withColumn("insider_user_flag", coalesce($"insider_user_flag_phone", $"insider_user_flag_email"))
      .withColumn("can_receive_email", when($"Receive_email" === 0, lit(0)).otherwise(lit(1))).drop("Receive_email")
      .na.fill(Map(
        "KYC_completion" -> 0,
        "is_email_verified" -> 0,
        "can_receive_email" -> 1,
        "is_gold_customer" -> "no",
        "PAYTMFIRST_status" -> "NO",
        "ticketnew_user_flag" -> 0,
        "insider_user_flag" -> 0
      ))
      .drop("alipay_user_id", "user_phone", "user_email", "delivery_tel", "delivery_email")

    profile.saveParquet(profilePath, 500)
  }

  def ticketnewUserPhone(spark: SparkSession, settings: Settings): DataFrame = {
    val ticketnewUsers = DataTables.ticketnewUsers(spark, settings.featuresDfs.ticketnewUsers)
    ticketnewUsers
      .select(col("user_phone"), lit(1) as "ticketnew_user_flag_phone")
      .distinct()
  }

  def ticketnewUserEmail(spark: SparkSession, settings: Settings): DataFrame = {
    val ticketnewUsers = DataTables.ticketnewUsers(spark, settings.featuresDfs.ticketnewUsers)
    ticketnewUsers
      .select(col("user_email"), lit(1) as "ticketnew_user_flag_email")
      .distinct()
  }

  def insiderUserPhone(spark: SparkSession, settings: Settings): DataFrame = {
    val insiderUserTransactions = DataTables.insiderUserTransactions(spark, settings.featuresDfs.insiderUsers)
    insiderUserTransactions
      .select(col("delivery_tel"), lit(1) as "insider_user_flag_phone")
      .distinct()
  }

  def insiderUserEmail(spark: SparkSession, settings: Settings): DataFrame = {
    val insiderUserTransactions = DataTables.insiderUserTransactions(spark, settings.featuresDfs.insiderUsers)
    insiderUserTransactions
      .select(col("delivery_email"), lit(1) as "insider_user_flag_email")
      .distinct()
  }

  def getLatestTablePath(spark: SparkSession, basePath: String, endDate: String): String = {
    val fs = FileSystem.get(new java.net.URI(basePath), spark.sparkContext.hadoopConfiguration)
    val pathPattern = new Path(basePath + "/*/_SUCCESS")
    val allPaths = fs.globStatus(pathPattern)
    basePath + "/" + allPaths.map(elem => elem.getPath.toString.split("/").takeRight(2).head).filter(date => date.replace("dt=", "") <= endDate).max
  }

  def platformSpecificSignupDateFeature(settings: Settings)(implicit spark: SparkSession): DataFrame = {

    // INPUT
    readTableV3(spark, settings.datalakeDfs.admAuditRecord).createOrReplaceTempView("adm_audit_record_snapshot")
    readTableV3(spark, settings.datalakeDfs.admCuCommonUniqueKey).createOrReplaceTempView("adm_cu_common_unique_key_snapshot")
    spark.read.option("header", "true").csv(s"${settings.featuresDfs.resources}/external_client_id_mapping/").createOrReplaceTempView("external_client_id_mapping")

    // SQL query
    spark.sql(
      s"""
         |select
         |    t2.value as customer_id,
         |    struct(max(to_date(t1.created_time)) as sign_up_date, last(exid.external_client_id) as external_client_id) as external_sign_up_date
         |FROM adm_audit_record_snapshot t1
         |inner join external_client_id_mapping exid
         |    on exid.client_id = t1.third_client_id
         |left join adm_cu_common_unique_key_snapshot t2
         |    on t1.actor_id = t2.principal_id
         |where
         |    lower(t1.action_name) = "usr_user_signup"
         |    and t2.value is not null
         |group by t2.value
        """.stripMargin
    )
  }
}
