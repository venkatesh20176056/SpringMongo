package com.paytm.map.featuresplus.rechargeprediction

import com.paytm.map.features.utils.ArgsUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, length, udf, when}
import org.apache.spark.sql.types.IntegerType
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import com.paytm.map.featuresplus.rechargeprediction.Constants.{ageLimit, ageMissingEncoding}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.hadoop.fs.Path
object Utility {

  /** case classes **/
  case class DateBound(date: org.joda.time.DateTime, lookBackDays: Int) {
    val startDate: String = date.minusDays(lookBackDays).toString(ArgsUtils.formatter)
    val endDate: String = date.toString(ArgsUtils.formatter)
  }

  class SelectAndFilterRuleClass(primaryCols: Seq[Column], filterCondition: Seq[Column], conditionalCols: Seq[Column], selectCols: Seq[Column]) {
    val rowsConditions: Column = (primaryCols ++ filterCondition).reduce(_ && _)
    val colsCondition: Seq[Column] = conditionalCols ++ selectCols
  }

  /** udfs **/
  class UDFClass {

    val cleanPhoneNo: UserDefinedFunction = udf((phone_no: String) => {
      phone_no match {
        case s if phone_no.split(" ").length > 1 => phone_no.split(" ").last
        case s if phone_no.split("-").length > 1 => phone_no.split("-").last
        case _                                   => phone_no
      }
    })

    val trimAdLowerCase: UserDefinedFunction = udf((str: String) => { str.trim.toLowerCase() })

    val removeSpace: UserDefinedFunction = udf((str: String) => str.replace(" ", "_"))

    val getTimeDelta: UserDefinedFunction = udf((lagDate: String, thisDate: String) => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      Days.daysBetween(formatter.parseDateTime(lagDate), formatter.parseDateTime(thisDate)).getDays
    })

    val addDays: UserDefinedFunction = udf((daysToAdd: Int, lastRechargeDateStr: String) => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val lastRechargeDate = formatter.parseDateTime(lastRechargeDateStr)
      lastRechargeDate.plusDays(daysToAdd).toString("yyyy-MM-dd")
    })
  }

  val UDF = new UDFClass

  /** other functions **/
  class FunctionClass {
    def conditionPrimaryCol(arr: Seq[Column]): Seq[Column] = arr.map(r => r.isNotNull && (length(r) > 0))

    def getLatest(path: String, date: String, sep: String = "/date="): String = {
      import org.apache.hadoop.fs.{FileSystem, Path}
      import org.apache.hadoop.conf.Configuration

      val fs = FileSystem.get(new java.net.URI(path), new Configuration())
      val file = fs.listStatus(new Path(path)).
        filter(elem => elem.getPath.toString.split(sep).last <= s"$date").
        map(elem => elem.getPath.toString).max
      file
    }

    def checkFileExists(spark: SparkSession, filePath: String): Boolean = {
      val path = new Path(s"$filePath/_SUCCESS")
      path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)
    }
  }

  val Function = new FunctionClass

  /** Table properties **/
  // todo: expose only a subset of all items that is here i.e. make others private
  class TableConditionClass {

    val soiFilterCondition = Seq(col("status") === 7, col("vertical_id") === 4)
    val soiConditionsCols = Seq()
    val soiPrimaryCols = Seq()
    val soiSelectCols = Seq(
      col("order_item_id"),
      col("order_id"),
      col("product_id"),
      col("selling_price"),
      col("created_at"),
      col("discount"),
      col("dt").as("date")
    )

    val soiConfig = new SelectAndFilterRuleClass(soiPrimaryCols, soiFilterCondition, soiConditionsCols, soiSelectCols)

    val soFilterConditions = Seq(col("payment_status") === 2)
    val soPrimaryCol = Seq()
    val soConditionsCols = Seq()
    val soSelectCols = Seq(col("order_id"), col("customer_id"))

    val soConfig = new SelectAndFilterRuleClass(soPrimaryCol, soFilterConditions, soConditionsCols, soSelectCols)

    val rechargePrimaryCols: Seq[Column] = Function.conditionPrimaryCol(Seq(col("recharge_number_1")))
    val rechargeFilterConditions: Seq[Column] = Seq(
      col("service") === "mobile",
      col("paytype") === "prepaid"
    )
    val rechargeConditionalCols: Seq[Column] = Seq(
      UDF.removeSpace(UDF.trimAdLowerCase(col("operator"))).as("operator"),
      UDF.removeSpace(UDF.trimAdLowerCase(col("circle"))).as("circle"),
      UDF.cleanPhoneNo(col("recharge_number_1")) as "ben_phone_no"
    )
    val rechargeSelectCols: Seq[Column] = Seq(
      col("order_item_id"),
      col("amount").as("recharge_amount")
    )

    val rechargeConfig =
      new SelectAndFilterRuleClass(rechargePrimaryCols, rechargeFilterConditions, rechargeConditionalCols, rechargeSelectCols)

    val profilePrimaryCols: Seq[Column] = Function.
      conditionPrimaryCol(Seq(col("phone_no"), col("customer_id")))
    val profileFilterCondition = Seq()
    val profileConditionalCols = Seq(
      UDF.cleanPhoneNo(col("phone_no")) as "reg_phone_no",
      when(
        col("age").isNotNull &&
          (col("age") geq ageLimit.min) &&
          (col("age") leq ageLimit.max), col("age")
      )
        .otherwise(ageMissingEncoding).as("age")
    )
    val profileSelectCols = Seq(
      col("customer_id"),
      col("is_email_verified").cast(IntegerType).as("is_email_verified")
    )

    val profileConfig =
      new SelectAndFilterRuleClass(profilePrimaryCols, profileFilterCondition, profileConditionalCols, profileSelectCols)

  }

  val TableCondition = new TableConditionClass

}
