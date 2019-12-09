package com.paytm.map.featuresplus.genderestimation

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features._
import com.paytm.map.features.utils.ConvenientFrame._
import com.paytm.map.features.utils.UDFs.{getMinMax, getPaytmAPlusMapping, readTableV3}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.featuresplus.StringUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object FirstNameToGender extends FirstNameToGenderJob
  with SparkJob with SparkJobBootstrap

trait FirstNameToGenderJob {
  this: SparkJob =>

  val JobName = "FirstNameToGender"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import GEModelConfigs._
    import settings._
    import spark.implicits._

    // Input Args and Paths
    val targetDate: DateTime = ArgsUtils.getTargetDate(args)
    val targetDateStr: String = targetDate.toString(ArgsUtils.formatter)

    // Output Paths
    val customerToFirstNamePath = s"${featuresDfs.featuresPlusData}/customerToFirstName/"
    val firstNameToGenderPath = s"${featuresDfs.featuresPlusData}/firstNameToGender/"

    // Read ouath table with aplus customer_ids
    val aplusOauth = readTableV3(spark, datalakeDfs.dwhCustRegnPath, targetDateStr)
      .select(
        $"customer_registrationid" as "alipay_user_id",
        $"first_name",
        $"gender"
      )
      .filter($"first_name".isNotNull && $"gender".isNotNull)
      .filter($"first_name" =!= "" && $"gender" =!= "")
      .filter($"first_name" =!= "null")

    // Read aplus to paytm customer_id mapping
    val paytmAPlusMapping = getPaytmAPlusMapping(spark, datalakeDfs.paytmAPlusMapping, targetDateStr)

    // Get paytm customer ids
    val paytmOauth = paytmAPlusMapping
      .join(aplusOauth, Seq("alipay_user_id"))
      .drop("alipay_user_id")
      .filter($"gender".isNotNull)

    // Clean first names
    val customerToFirstName = paytmOauth
      .withColumn("first_name", trim(lower($"first_name")))
      .withColumn("first_name", keepOnlyFirstWord($"first_name"))
      .withColumn("first_name", dedupeCommonVariations($"first_name"))
      .withColumn("first_name", replaceLastAlphabet($"first_name", lit("y"), lit("i")))
      .filter($"first_name" =!= "")
      .filter(isNoVowelName($"first_name"))
      .filter(isNonAlphabets($"first_name"))
      .filter(isSingleAlphabets($"first_name"))

    // Save the cleaned first names
    customerToFirstName.saveParquet(customerToFirstNamePath, withRepartition = false)

    val totalCount = customerToFirstName.count

    /*
    Raw Probability Logic:
    - if male_count > female_count, p = male_count/name_count
    - else if female_count > male_count, p = female_count/name_count
    - else, p = 0.5

    Confidence Logic:
    - Greater the name_count, more common the name. Thus more confidence we would be about its gender
    - Since name_count/ is positively skewed distribution, we take its log to distribute it evenly
    - Since log would give negative values too, scale it to [0,1] range using minMax scaling

    Probability Logic:
    Raw Probability * Confidence
     */

    // Get gender counts for each first name, transform counts to log(counts)
    val nameCounts = customerToFirstName
      .groupBy("first_name")
      .agg(
        sum(when($"gender" === 1, 1).otherwise(0)) as "male_count",
        sum(when($"gender" === 2, 1).otherwise(0)) as "female_count",
        sum(when($"gender" =!= 1 && $"gender" =!= 2, 1).otherwise(0)) as "other_count"
      )
      .withColumn("name_count", $"male_count" + $"female_count" + $"other_count")
      .withColumn("name_log", log10($"name_count" / lit(totalCount)))

    val (minNameLog, maxNameLog) = getMinMax(nameCounts, "name_log")
    val diffNameLog = maxNameLog - minNameLog

    val firstNameToGender = nameCounts
      .withColumn(
        "rawProbability",
        when($"male_count" > $"female_count", $"male_count" / $"name_count")
          .otherwise(
            when($"female_count" > $"male_count", $"female_count" / $"name_count")
              .otherwise(0.5)
          )
      )
      .withColumn("confidence", ($"name_log" - lit(minNameLog)) / lit(diffNameLog))
      .withColumn("prediction", when($"male_count" >= $"female_count", maleLabel).otherwise(femaleLabel))
      .withColumn("probability", round($"rawProbability" * $"confidence", scale = 5))

    // Save the lookup
    firstNameToGender.saveParquet(firstNameToGenderPath, withRepartition = false)

  }
}