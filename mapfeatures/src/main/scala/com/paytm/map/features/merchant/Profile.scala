package com.paytm.map.features.merchant

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.utils.UDFs._
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.joda.time.DateTime

object ProfileJob extends Profile with SparkJob with SparkJobBootstrap

trait Profile {

  this: SparkJob =>

  val JobName = "MerchantProfile"

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]) = {
    import spark.implicits._

    implicit val imSpark = spark

    val targetDate = ArgsUtils.getTargetDate(args)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)

    val cityState = cleanedCityStateMappings(settings)
      .drop("zone")

    val ccAbuser = ccAbuserMapping(settings)
      .withColumn("cc_abuser", lit(1))

    val cashbackAbuser = cashbackAbuserMapping(settings)
      .withColumn("cashback_abuser", lit(1)).drop("customer_id")

    //To map merchant_id to customer_id
    val uidEidMapping = DataTables.UIDEIDMapper(spark, settings.datalakeDfs.uidEidMapper)
      .filter($"isActive" === 1)
      .select("entity_id", "customer_id")

    val factTablePaths = settings.datalakeDfs.factOffMerchant
    val customerOlapPaths = settings.datalakeDfs.customerOlap
    val merchantUserPaths = settings.datalakeDfs.merchantUser
    val inputDirectory = settings.datalakeDfs.offOrgMerchant

    val outputPath = s"${settings.featuresDfs.baseDFS.aggPath}/MerchantProfile"

    val inputDF = readTableV3(spark, inputDirectory)

    val factTable = readTableV3(spark, factTablePaths)
      .where($"customer_id".isNotNull and $"current_link" === "Y")
      .select(
        $"merchant_id",
        $"customer_id",
        lit("1").alias("ppb_linked")
      )

    val customerOlap = readTableV3(spark, customerOlapPaths)
      .select(
        $"cust_id".alias("customer_id"),
        to_date($"acct_opn_date").alias("ppb_opened"),
        to_date($"debit_card_issue_date").alias("debit_card_issue")
      )

    val window = Window.partitionBy($"pg_mid").orderBy($"update_timestamp".desc)
    val merchantUser = readTableV3(spark, merchantUserPaths)
      .select(
        $"pg_mid",
        first($"contact_phone_no").over(window) as "contact_phone_no",
        first($"secondary_contact_phone_no").over(window) as "secondary_contact_phone_no",
        first($"create_timestamp").over(window) as "create_timestamp"
      )
      .select(
        $"pg_mid".alias("merchant_id"),
        when($"contact_phone_no".isNotNull, $"contact_phone_no").otherwise(null).alias("phone_no"),
        $"secondary_contact_phone_no",
        to_date($"create_timestamp").alias("signup_date")
      )
      .dropDuplicates("merchant_id")

    val childMidDF = childMidList(inputDF)

    val profileFeatures = inputDF.select(
      $"pg_mid".alias("merchant_id"),
      lower($"agent_type").alias("agent_type"),
      lower($"pg_category").alias("category"),
      lower($"pg_sub_category").alias("sub_category"),
      $"pg_isaggregator",
      trim(lower($"pg_state_name")).alias("state"),
      trim(when(lower($"pg_main_city").isin("others", "delhi-ncr"), lower($"pg_city")).otherwise(lower($"pg_main_city"))).alias("city"),
      when($"pg_status" === 9376503, 1).otherwise(0).alias("migrated_to_offline_pg"),
      lower(mid_type_column).alias("mid_type"),
      aggregator_mid.alias("aggregator_mid"),
      when($"pg_isChild" === 1, 1).otherwise(0).alias("isChild"),
      $"pg_entity_id".alias("entity_id"),
      $"pg_pincode".alias("pincode"),
      when($"w_is_ppilimitedmerchant" =!= 0, 1).otherwise(0).alias("is_self_declared")
    )
      .withColumn("isAggregator", when($"pg_isaggregator" === 1, "true").otherwise("false"))
      .drop("pg_isaggregator")

    val installedAppsFeatures = installedAppsFeature(settings, targetDate, uidEidMapping)
    val edcFeatures = edcFeature(settings)
    val edcOnboardingFeatures = edcOnboradingFeature(settings)

    val output = profileFeatures
      .join(factTable, Seq("merchant_id"), "left_outer")
      .join(customerOlap, Seq("customer_id"), "left_outer")
      .join(merchantUser, Seq("merchant_id"), "left_outer")
      .drop("customer_id")
      .join(childMidDF, Seq("merchant_id"), "left_outer")
      .join(cityState, Seq("city"), "left_outer")
      .join(cashbackAbuser, Seq("merchant_id"), "full_outer")
      .join(uidEidMapping, Seq("entity_id"), "left_outer")
      .join(installedAppsFeatures, Seq("customer_id"), "left_outer")
      .join(edcFeatures, Seq("merchant_id"), "left_outer")
      .join(edcOnboardingFeatures, Seq("customer_id"), "outer")
      .join(ccAbuser, Seq("customer_id"), "full_outer")
      .drop("entity_id")

    output.repartition(200).write.mode(SaveMode.Overwrite).parquet(s"$outputPath/dt=$targetDateStr")

  }

  def validate(spark: SparkSession, settings: Settings, args: Array[String]) = {
    DailyJobValidation.validate(spark, args)
  }

  val mid_type_column: Column = when(col("w_is_ppilimitedmerchant") === 1, "50K")
    .otherwise(when(col("w_is_ppilimitedmerchant") === 2, "5L")
      .otherwise(when(col("w_is_ppilimitedmerchant") === 3, "1L")
        .otherwise("unlimited")))

  val aggregator_mid: Column = when(col("pg_isChild") === 1, col("pg_aggregator_mid")).otherwise(null)

  def childMidList(profileDF: DataFrame): DataFrame = {
    profileDF.filter(col("pg_isChild") === 1)
      .select(col("pg_mid"), col("pg_aggregator_mid").alias("merchant_id"))
      .groupBy("merchant_id")
      .agg(collect_set("pg_mid").alias("child_mids"))
  }

  def standarizePhoneNo(phoneNumber: Column): Column = {
    when(length(phoneNumber) === 10 and isAllNumeric(phoneNumber), concat(lit("91-"), phoneNumber)).otherwise(null)
  }

  def cleanedCityStateMappings(settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val cityMappingSchema = StructType(Array(
      StructField("city", StringType, true),
      StructField("mapped_city", StringType, true),
      StructField("state", StringType, true),
      StructField("zone", StringType, true)
    ))

    try {
      spark.read.option("delimiter", "|").csv(settings.featuresDfs.merchantCityStateMapping)
        .select($"_c0" as "city", $"_c1" as "cleaned_city", $"_c2" as "cleaned_state", $"_c3" as "zone")
    } catch {
      case e: Throwable =>
        log.warn("Can't read city mapping data")
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], cityMappingSchema)
    }
  }
  def cashbackAbuserMapping(settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val cashbackAbuserSchema = StructType(Array(
      StructField("customer_id", LongType, true),
      StructField("merchant_id", StringType, true)
    ))

    try {
      spark.read.option("delimiter", "|").csv(settings.featuresDfs.cashbackAbuserMapping)
        .select($"_c0".cast(LongType) as "customer_id", $"_c1" as "merchant_id")
    } catch {
      case e: Throwable =>
        log.warn("Can't read cashback Abuser Mapping data", e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], cashbackAbuserSchema)
    }
  }

  def ccAbuserMapping(settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ccAbuserSchema = StructType(Array(
      StructField("customer_id", LongType, true)
    ))

    try {
      spark.read.option("delimiter", "|").csv(settings.featuresDfs.ccAbuserMapping)
        .select($"_c0".cast(LongType) as "customer_id")
    } catch {
      case e: Throwable =>
        log.warn("Can't read CC Abuser Mapping data", e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ccAbuserSchema)
    }
  }

  def installedAppsFeature(settings: Settings, targetDate: DateTime, custToMerchantMapping: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val deviceFeaturesDF = spark.read.parquet(s"${settings.featuresDfs.baseDFS.aggPath}/DeviceFeatures")
      .join(custToMerchantMapping, Seq("customer_id"), "left_semi")
    val installedAppsColName = "installed_apps_list"

    val minUsers = 5000
    val daysBack = 30
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
    }

    deviceFeaturesDF
      .getFirstLast(Seq("customer_id"), "dt", Seq(s"$installedAppsColName as $installedAppsColName"), false)
      .select(
        explode(col(installedAppsColName)) as "app",
        $"customer_id"
      )
      .join(topApps, Seq("app"))
      .groupBy($"customer_id")
      .agg(
        collect_set($"app") as installedAppsColName
      )
  }

  def edcFeature(settings: Settings)(implicit spark: SparkSession): DataFrame = {

    readTableV3(spark, settings.datalakeDfs.entityEDCInfo).createOrReplaceTempView("entity_edc_info")

    spark.sql(
      """
        |select
        | mid as merchant_id,
        | case when min(modified_date) > min(created_date) then "YES" else "NO" end as mapped,
        | case when min(modified_date) > min(created_date) then min(modified_date) else null end as last_device_mapped_date
        |from (
        |  select
        |      p.device_serial_no as device_serial_no,
        |      p.mid as mid,
        |      p.monthly_rental as monthly_rental,
        |      p.terminal_status as terminal_status,
        |      p.terminal_status_msg as terminal_status_msg,
        |      p.vendor_name as vendor_name,
        |      p.model_name as model_name,
        |      p.created_date as created_date,
        |      p.modified_date as modified_date,
        |      rank() over(partition by p.device_serial_no order by p.modified_date, p.created_date) as rank
        |      from entity_edc_info p
        |      where p.mid not like '%Abhish%'
        |) t1
        |where t1.rank = 1
        | and t1.terminal_status in ('ACTIVE','PENDING_VERIFICATION')
        |group by t1.mid
        |""".stripMargin
    )
      .renameColumns("edc_", Seq("merchant_id"))
  }

  def edcOnboradingFeature(settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    readTableV3(spark, settings.datalakeDfs.merchantOnboardingVubm).createOrReplaceTempView("v_ubm")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingRelatedBusinessSolution).createOrReplaceTempView("related_business_solution_mapping")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingSolutionAdditionalInfo).createOrReplaceTempView("solution_additional_info")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingWorkflowStatus).createOrReplaceTempView("workflow_status")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingWorkflowNode).createOrReplaceTempView("workflow_node")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingSolutionDoc).createOrReplaceTempView("solution_doc")
    readTableV3(spark, settings.datalakeDfs.merchantOnboardingFieldRejectionReason).createOrReplaceTempView("field_rejection_reason")

    val rejectedReason = spark.sql(
      """
        |SELECT
        | a.cust_id as customer_id,
        | collect_set( struct(d.rejected_field_reason, e.doc_type) ) as edc_rejected_reason
        |FROM v_ubm as a
        |left join workflow_status AS b on a.id=b.user_business_mapping_id
        |left join  field_rejection_reason d  on b.id=d.workflow_status_id
        |left join  solution_doc AS e on d.rejected_field_name=e.doc_provided
        |left join related_business_solution_mapping rbsm    on  a.rdsmid=rbsm.id
        |left join
        |    ( select * from
        |     solution_additional_info
        |     where solution_key in ('MAP_EDC_REQUEST_MID','PG_MID')
        |    ) sai_2
        |  on rbsm.solution_id=sai_2.solution_id
        |WHERE  a.solution_type='qr_merchant'
        |and rejected_field_reason is not null
        |group by customer_id
        |""".stripMargin
    )

    val edcPlan = spark.sql(
      """
        |SELECT
        | ubm.cust_id AS Merchant_Cust_ID,
        | sai.updated_at as Payment_date,
        | sai_2.solution_value as mid,
        | ubm.creator_cust_id AS Agent_Cust_id,
        | wn.sub_stage as stage,
        | ubm.created_at as Lead_Creation_Timestamp,
        | ws.updated_at as Updated_at,
        | ubm.solution_type as Solution_type,
        | get_json_object(sai.solution_value,'$.amount') as txn_amount,
        | get_json_object(sai_3.solution_value,'$.rentalType') as plan_name,
        | get_json_object(sai.solution_value,'$.orderId') as Order_id
        |FROM v_ubm as ubm
        |left join related_business_solution_mapping rbsm
        |on  ubm.rdsmid=rbsm.id
        |left join solution_additional_info sai
        |on rbsm.solution_id=sai.solution_id
        |left join workflow_status ws
        |on ubm.id = ws.user_business_mapping_id
        |left join workflow_node wn
        |on ws.workflow_node_id = wn.id
        |left join
        | ( select * from
        | solution_additional_info
        | where solution_key in ('MAP_EDC_REQUEST_MID','PG_MID')
        | ) sai_2
        |on rbsm.solution_id=sai_2.solution_id
        |left join
        |(
        |select * from solution_additional_info
        | where solution_key in ('EDC_ORDER_DETAILS')
        |) sai_3
        |on rbsm.solution_id=sai_3.solution_id
        |WHERE  ws.is_active = '1'  and ubm.solution_type IN ( 'map_edc') AND sai.solution_key in ('QR_CODE_AND_TRANSACTION_DETAILS')
        |""".stripMargin
    )
      .select(
        $"Merchant_Cust_ID" as "customer_id",
        $"plan_name" as "edc_rental_plan",
        $"txn_amount" as "edc_rental_amount"
      )
      .dropDuplicates("customer_id")

    rejectedReason.join(edcPlan, Seq("customer_id"), "outer")
  }
}
