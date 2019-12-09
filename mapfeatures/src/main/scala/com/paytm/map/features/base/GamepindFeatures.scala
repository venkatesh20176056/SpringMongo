package com.paytm.map.features.base

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.DataTables.DFCommons
import com.paytm.map.features.config.Schemas.SchemaRepo
import com.paytm.map.features.utils.UDFs.readTableV3
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import com.paytm.map.features.utils.ConvenientFrame.LazyDataFrame

object GamepindFeatures extends GamepindFeaturesJob
  with SparkJob with SparkJobBootstrap

trait GamepindFeaturesJob {
  this: SparkJob =>

  val JobName = "GamepindFeatures"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    implicit val sparkSession = spark

    // Get the command line parameters
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val lookBackDays: Int = args(1).toInt
    val startDate: DateTime = targetDate.minusDays(lookBackDays)
    val targetDateStr = targetDate.toString(ArgsUtils.formatter)
    val startDateStr = startDate.toString(ArgsUtils.formatter)
    val dtSeq = BaseTableUtils.dayIterator(targetDate.minusDays(lookBackDays), targetDate, ArgsUtils.formatter)

    // Output path
    val anchorPath = s"${settings.featuresDfs.baseDFS.anchorPath}/Gamepind"
    val outPath = s"${settings.featuresDfs.baseDFS.aggPath}/GamepindFeatures"

    val groupByColumns = Seq("customer_id", "dt")
    val dataDF = {
      gamepindFeatures(settings, startDateStr, targetDateStr)
        .join(rummyTotalGamesPlayed(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(
          xengageFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(overallClickAggregates(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(sourceClickAggregates(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(beansFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(battleCenterFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(beanPurchasedFeatures(settings, startDateStr, targetDateStr), Seq("customer_id"), "outer")
        .join(transactionalLoggingFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(gamepindOverallTxnFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(gamepindRevTypeFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(gamepindGameTypeFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(userSpsTransactionFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .join(
          casFeatures(settings, startDateStr, targetDateStr), groupByColumns, "outer")
        .addPrefixToColumns("GAMEPIND_", groupByColumns)
        .alignSchema(SchemaRepo.GamepindSchema, requiresExistence = true)
        .coalescePartitions("dt", "customer_id", dtSeq)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("dt")
        .parquet(anchorPath)
      spark.read.parquet(anchorPath)
    }

    dataDF.moveHDFSData(dtSeq, outPath)
  }

  def gamepindOverallTxnFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val transDF = readTableV3(spark, datalakeDfs.gamepindTransactions, targetDateStr, startDateStr)

    val aggPlayerDF = transDF
      .withColumn("dt", to_date($"tdate"))
      .groupBy($"playerid", $"dt")
      .agg(max($"dt") as "last_game_played_date", count($"playerid") as "game_played_count")
      .select($"playerid" as "customer_id", $"dt", $"last_game_played_date", $"game_played_count")

    aggPlayerDF
  }

  def gamepindRevTypeFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val transDF = readTableV3(spark, datalakeDfs.gamepindTransactions, targetDateStr, startDateStr)

    val revTypes = Seq("free", "beans", "cash", "chips")
    val aggRevPlayerDf = transDF
      .withColumn("dt", to_date($"tdate"))
      .withColumn("rev_type", lower($"rev_type"))
      .withColumn("rev_type", when($"rev_type" === "beanscoin", "beans")
        .otherwise(when($"rev_type" === "cashcoin", "cash").otherwise($"rev_type")))
      .groupBy($"playerid", $"dt")
      .pivot("rev_type", revTypes)
      .agg(
        count($"playerid") as "game_played_count",
        max($"dt") as "last_game_played_date"
      )
      .withColumnRenamed("playerid", "customer_id")

    aggRevPlayerDf
  }

  def gamepindGameTypeFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val transDF = readTableV3(spark, datalakeDfs.gamepindTransactions, targetDateStr, startDateStr)

    val gameTypes = Seq("free_contest", "beans_contest", "cash_contest")
    val aggGamePlayerDF = transDF
      .withColumn("dt", to_date($"tdate"))
      .withColumn("game_type", lower(regexp_replace($"game_type", " ", "_")))
      .groupBy($"playerid", $"dt")
      .pivot("game_type", gameTypes)
      .agg(
        count($"playerid") as "game_played_count",
        max(to_date($"dt")) as "last_game_played_date"
      )
      .withColumnRenamed("playerid", "customer_id")

    aggGamePlayerDF

  }

  def transactionalLoggingFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val transactionalLoggingDf = readTableV3(spark, datalakeDfs.taskManagerTransactionalLogging, targetDateStr, startDateStr)
      .withColumn("dt", to_date($"created_date"))
      .repartition($"customer_id", $"dt")
      .cache

    val eventCodeTypes = Seq("CMS-GAME-PLAY", "INAPP-GAME-PLAY")
    val eventCodeAggDf = transactionalLoggingDf
      .where($"event_code".isin(eventCodeTypes: _*))
      .groupBy($"customer_id", $"dt")
      .agg(
        count("*") as "games_played_through_task_center",
        max("dt") as "last_game_played_through_task_center_date"
      )

    val activityTypes = Seq("daily_bonus_claimed")
    val activityTypeAggDf = transactionalLoggingDf
      .where($"activity_type".isin(activityTypes: _*))
      .groupBy($"customer_id", $"dt")
      .agg(
        count("*") as "visiting_bonus_claim_count",
        max("dt") as "last_visiting_bonus_claim_date"
      )

    val actionTypes = Seq("complete")
    val actionTypeAggDf = transactionalLoggingDf
      .where($"action".isin(actionTypes: _*))
      .groupBy($"customer_id", $"dt")
      .agg(
        count("*") as "task_completed_count",
        max("dt") as "last_task_completed_date"
      )

    eventCodeAggDf
      .join(activityTypeAggDf, Seq("customer_id", "dt"))
      .join(actionTypeAggDf, Seq("customer_id", "dt"))
  }

  def gamepindFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val gameIds = List(9, 10, 12, 13, 14, 16)
    val gamepindPreJoin = readTableV3(spark, datalakeDfs.gamepindTrans)
      .where($"pay_status" === 1)
      .withColumn("dt", to_date($"gmt_create"))
      .where($"game_id".isin(gameIds: _*))
      .where($"dt".between(startDateStr, targetDateStr))

    // user_id is phone number, join with oauth
    val maskedMobileNumberToCustId = maskedMobileNumberToCustIdMapping(settings)
    val gamepind = gamepindPreJoin
      .join(maskedMobileNumberToCustId, $"user_id" === $"masked_mobile_number", "inner")

    val gameNameMap = Seq(
      (9, "WHEELOFFORTUNE"),
      (10, "TREASUREHUNT"),
      (12, "JUNGLEADVENTURE"),
      (13, "BIGORSMALL"),
      (14, "TRIPLESIX"),
      (16, "ZOORUN")
    )
    val gameNamesDF = gameNameMap.toDF("game_id", "game_name")

    val groupByColumns = Seq("customer_id", "dt")
    val statsColumns = Seq(
      count($"customer_id") as "total_games_played",
      sum(when($"win_fee" > 0, 1).otherwise(0)) as "win_count",
      sum(when($"win_status" === 2, $"fee").otherwise(0)) as "beans_won",
      sum(when($"win_status" === 1, $"fee").otherwise(0)) as "beans_lost"
    )

    val gameStat = gamepind
      .join(broadcast(gameNamesDF), "game_id")
      .groupBy(groupByColumns.head, groupByColumns.tail: _*)
      .pivot("game_name", gameNameMap.map(_._2))
      .agg(statsColumns.head, statsColumns.tail: _*)

    val overallStat = gamepind
      .groupBy(groupByColumns.head, groupByColumns.tail: _*)
      .agg(statsColumns.head, statsColumns.tail: _*)
      .addPrefixToColumns("OVERALL_", groupByColumns)

    overallStat
      .join(gameStat, groupByColumns)
  }

  def maskedMobileNumberToCustIdMapping(settings: Settings)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    readTableV3(spark, datalakeDfs.casUser)
      .select(
        first($"masked_mobile_number").over(Window.partitionBy($"customer_id").orderBy($"created".desc)) as "masked_mobile_number",
        $"customer_id"
      )
      .dropDuplicates
      .cache
  }
  def casFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._
    val usersDF = readTableV3(spark, datalakeDfs.casUser, targetDateStr, startDateStr)
    usersDF.withColumn("dt", to_date($"created"))
      .groupBy($"customer_id", $"dt")
      .agg(first($"creation_source") as "onboarding_channel",
        max(when($"customer_id".isNotNull, 1).otherwise(0)) as "user_onboarded")

  }

  def overallClickAggregates(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._
    val gameServeHistory = readTableV3(spark, datalakeDfs.ppProfilerGameServeHistory, targetDateStr, startDateStr)

    gameServeHistory
      .withColumn("dt", to_date($"created"))
      .groupBy($"customer_id", $"dt")
      .agg(
        count($"customer_id") as "click_count",
        max($"dt") as "last_click_date",
        min($"dt") as "first_click_date",
        sum(when($"product_id" === 205673607, 1).otherwise(0)) as "fantasy_cash_click_count",
        sum(when($"product_id" === 239187597, 1).otherwise(0)) as "fantasy_points_click_count",
        max(when($"product_id" === 205673607, $"dt").otherwise(null)) as "fantasy_cash_pid_last_click_date",
        max(when($"product_id" === 239187597, $"dt").otherwise(null)) as "fantasy_points_pid_last_click_date"
      )
  }

  def xengageFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._
    val appOpenHistory = readTableV3(spark, datalakeDfs.xengageWeUserGcm, targetDateStr, startDateStr)

    appOpenHistory
      .withColumn("dt", to_date($"updated_on"))
      .groupBy($"customer_id",$"dt")
      .agg(
        max($"is_gcm_valid").alias("user_gcm_id"),
        max(when(($"app_id" === 3 and $"customer_id".isNotNull), 1).otherwise(0)).alias("pro_app_opened")
      )
  }

  def sourceClickAggregates(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val gameServeHistory = readTableV3(spark, datalakeDfs.ppProfilerGameServeHistory, targetDateStr, startDateStr)
      .select(
        $"customer_id",
        upper($"source") as "source",
        to_date($"created") as "dt"
      )

    val sources = Seq("POWERPLAY", "WEEX", "PAYTM_UPDATE_FEED", "PAYTM_ANDROID")
    val groupByCustomerAndSourceDf = gameServeHistory
      .where($"source".isin(sources: _*))
      .groupBy($"customer_id", $"dt")
      .pivot("source", sources)
      .agg(
        max($"dt") as "last_game_clicked_date",
        count("*") as "tmp"
      )
      .withColumnRenamed("PAYTM_ANDROID_last_game_clicked_date", "ANDROID_last_game_clicked_date")
      .withColumnRenamed("PAYTM_UPDATE_FEED_last_game_clicked_date", "UPDATEFEED_last_game_clicked_date") //For backwards compatibility

    groupByCustomerAndSourceDf
  }

  def beansFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val maskedMobileNumberToCustId = maskedMobileNumberToCustIdMapping(settings)

    val userAccount = readTableV3(spark, datalakeDfs.gamepindUserAccount)
      .join(maskedMobileNumberToCustId, $"user_id" === $"masked_mobile_number", "inner")
      .select(
        $"customer_id",
        $"balance_num",
        to_date($"gmt_modified") as "dt"
      )

    val runningTab = readTableV3(spark, datalakeDfs.gamepindRunningTab)
      .join(maskedMobileNumberToCustId, $"user_id" === $"masked_mobile_number", "inner")
      .select(
        $"customer_id",
        $"balance_num_before",
        $"balance_num_after",
        $"biz_amount",
        to_date($"gmt_create") as "dt"
      )

    val beanBalance = userAccount
      .select(
        $"customer_id",
        $"dt",
        $"balance_num" as "user_bean_balance"
      )

    val beansSpent = runningTab
      .where($"balance_num_before" > $"balance_num_after")
      .groupBy($"customer_id", $"dt")
      .agg(
        max($"dt") as "last_beans_spent_date",
        sum($"biz_amount") as "bean_spent_count"
      )
      .select(
        $"customer_id",
        $"dt",
        $"last_beans_spent_date",
        $"bean_spent_count"
      )

    beanBalance
      .join(beansSpent, Seq("customer_id", "dt"), "outer")
      .select(
        $"customer_id",
        $"dt",
        $"user_bean_balance",
        $"last_beans_spent_date",
        $"bean_spent_count"
      )
  }

  def beanPurchasedFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val maskedMobileNumberToCustId = maskedMobileNumberToCustIdMapping(settings)
    val creditData = readTableV3(spark, datalakeDfs.spsCreditData, targetDateStr, startDateStr)
      .join(maskedMobileNumberToCustId, $"masked_msisdn" === $"masked_mobile_number", "inner")
      .select($"customer_id", $"amount")

    val midTypes = Seq(1)
    val txnStatusTypes = Seq("SUCCESS")

    creditData
      .where($"mid".isin(midTypes: _*) && $"txn_status".isin(txnStatusTypes: _*))
      .groupBy($"customer_id")
      .agg(sum($"amount") as "total_beans_purchase")
      .select($"customer_id", $"total_beans_purchase")
  }

  def battleCenterFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {
    import settings._
    import spark.sqlContext.implicits._

    val maskedMobileNumberToCustId = maskedMobileNumberToCustIdMapping(settings)

    val billingHistory = readTableV3(spark, datalakeDfs.battleCenterPlayerBillingHistory, targetDateStr, startDateStr)
      .select(
        $"customer_id",
        to_date($"created_date") as "dt"
      )
    val txn = readTableV3(spark, datalakeDfs.battleCenterBattleTxn, targetDateStr, startDateStr)
      .join(maskedMobileNumberToCustId, $"cas_id" === $"masked_mobile_number", "inner")
      .select(
        $"customer_id",
        to_date($"created_date") as "dt"
      )
    val winners = readTableV3(spark, datalakeDfs.battleCenterWinners, targetDateStr, startDateStr)
      .select(
        $"customer_id",
        to_date($"created_date") as "dt"
      )

    val lastJoined = billingHistory
      .groupBy($"customer_id", $"dt")
      .agg(max($"dt") as "BC_last_join_date")

    val lastPlayed = txn
      .groupBy($"customer_id", $"dt")
      .agg(max($"dt") as "BC_last_play_date")

    val lastWin = winners
      .groupBy($"customer_id", $"dt")
      .agg(max($"dt") as "BC_last_win_date")

    lastJoined
      .join(lastPlayed, Seq("customer_id", "dt"), "outer")
      .join(lastWin, Seq("customer_id", "dt"), "outer")
      .select(
        $"customer_id",
        $"dt",
        $"BC_last_join_date",
        $"BC_last_play_date",
        $"BC_last_win_date"
      )
  }

  def userSpsTransactionFeatures(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {

    import settings._
    import org.apache.spark.sql.types.LongType
    import spark.sqlContext.implicits._

    val billingData = readTableV3(spark, datalakeDfs.spsBillingData, targetDateStr, startDateStr)
    val userData = readTableV3(spark, datalakeDfs.casUser, targetDateStr, startDateStr)

    val userdf = userData.withColumn("dt", to_date($"created")).select($"customer_id", $"dt", $"masked_mobile_number")
    val billingdf = billingData
      .withColumn("masked_msisdn", $"masked_msisdn".cast(LongType))
      .withColumn("billingDate", to_date($"created"))
      .select($"masked_msisdn", $"billingDate", $"billing_amount", $"mid")
      .where(($"cgresponsestatus" === "TXN_SUCCESS" || $"billing_status" === "freshCompleted" || $"billing_status" === "refunded") && ($"operator" === "PAYTM" || $"operator" === "GOOGLE"))

    val resDf = billingdf.join(userdf, $"masked_mobile_number" === $"masked_msisdn", "inner").
      groupBy($"customer_id", $"dt").
      agg(
        min($"billingDate") as "first_txn_date",
        max($"billingDate") as "last_txn_date",
        sum($"billing_amount") as "amount",
        count($"billingDate") as "user_txn_count",
        count(when($"mid" === 27, $"masked_msisdn").otherwise(null)) as "fantasy_txn_count",
        sum(when($"mid" === 27, $"billing_amount").otherwise(0)) as "fantasy_GMV",
        max(when($"mid" === 27, $"billingDate").otherwise(null)) as "fantasy_last_txn_date"
      )

    resDf
  }

  def rummyTotalGamesPlayed(settings: Settings, startDateStr: String, targetDateStr: String)(implicit spark: SparkSession): DataFrame = {

    import settings._
    import spark.sqlContext.implicits._

    val rummyDF = readTableV3(spark, datalakeDfs.rummyRcgGamePlayer, targetDateStr, startDateStr)
    val casDF = readTableV3(spark, datalakeDfs.casUser, targetDateStr, startDateStr)

    val df = rummyDF.as('a).join(casDF.as('b), $"a.msisdn" === $"b.masked_mobile_number")
    val resDF = df.withColumn("dt", $"a.join_time").groupBy($"b.customer_id").agg(countDistinct($"a.game_id").alias("games_played"))

    resDF
  }

}
