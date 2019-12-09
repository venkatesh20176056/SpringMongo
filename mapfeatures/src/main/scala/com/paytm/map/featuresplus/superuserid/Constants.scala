package com.paytm.map.featuresplus.superuserid

import com.paytm.map.featuresplus.superuserid.Utility.UDF
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._

object Constants {

  /** Case class **/
  case class VertexAttr(vertex_id: Long, customer_id: Long, super_user_id: Long, degree: Long, cluster_size: Long)
  case class EdgeAttr(similarity: Double)
  val packageName = "superuserid"

  /** paths **/
  class PathsClass(rootPath: String) extends Serializable {
    val projectBasePath = s"$rootPath$packageName"
    val checkpointDir = s"$projectBasePath/checkpoint/"

    val baseTablePath = s"$projectBasePath/baseTable"
    val allRelationsPath = s"$projectBasePath/allRelations"
    val exportPath = s"$projectBasePath/export/"
    val modelledRelationPath = s"$projectBasePath/modelledRelation/"

    val productionPath: String => String = (in: String) => in.replace("/stg/", "/prod/")
    val fraudData = productionPath(s"$projectBasePath/inData/fraud_relations/")
    val finalOutputPath = s"$rootPath/features/$packageName"

    val modelPath = s"$projectBasePath/relationsModel/"

    val firstModelDataPath = s"$checkpointDir/firstModelData/"
    val firstModelPath = s"$checkpointDir/firstModel/"
    val confidentLabeledDataPath = s"$checkpointDir/confidentLabeledData"
    val secondModelDataPath = s"$checkpointDir/secondModelDataPath/"

  }

  /** column names **/
  val columnConcat = "#$#"
  val frequency = "frequency"
  val similarity = "similarity"
  val customerIdCol = "customer_id"
  val inverseCustIdCol = "inverse_id"
  val superIdCol = "super_user_id"
  val sameSuperId = "has_same_super_id"

  val delPhoneCol = "delivery_phone"
  val bankAccCol = "account_no"
  val addressCol = "delivery_address"
  val deviceCol = "device_id"
  val ruContactCol = "beneficiary_no"
  val merchantBankCol = "merchant_account_no"
  val emailCol = "delivery_email"

  val similarCol = "similarValue"

  val initialLabelCol = "initialLabel"
  val initialPredCol = "initialPrediction"
  val probabCol = "probability"
  val modelFeaturesCol = "features"
  val confidentLabelCol = "confidentLabel"

  val identifierCols = Seq(s"${customerIdCol}_1", s"${customerIdCol}_2")

  /** CONSTANTS; will be tuned after parsed DT model **/
  val DEVICE_THRESHOLD = 2
  val RU_CONTACT_THRESHOLD = 2
  val ADDRESS_THRESHOLD = 2
  val BANK_ACCOUNT_THRESHOLD = 1
  val MERCHANT_BANK_THRESHOLD = 1
  val EMAIL_THRESHOLD = 1
  val DEL_PHONE_THRESHOLD = 1

  val OPEN_SYSTEM_THRESHOLD = 50
  val CLUSTER_SIZE_THRESHOLD = 100
  val LONG_MAX: Long = "10000000000".toLong

  val DIGIT_PRECISION = 2
  val MIN_THRESHOLD = 0.30
  val MAX_THRESHOLD = 0.95
  val positiveTag = 1.0
  val negativeTag = 0.0

  val strongFeaturesOrder = Seq(deviceCol, delPhoneCol, ruContactCol, addressCol, emailCol)
  /**
   * (bankAccCol, merchantBankCol): is removed since the data is not available for this
   * can be reverted once its available
   * To revert, just add `bankAccCol` to the  `strongFeaturesOrder` Seq
   */

  val age = "age"
  val gender = "gender"
  val kyc = "KYC_completion"
  val balance = "wallet_balance"
  val signUpDate = "sign_up_date"
  val emailVerified = "is_email_verified"
  val appLanguage = "latest_lang"

  val weakFeaturesOrder = Seq(age, gender, kyc, balance, signUpDate, emailVerified, appLanguage)

  case class Feature(featureName: String, frequency: String, similarity: String, threshold: Int, path: String)

  val featuresConfigMap = Map(
    delPhoneCol ->
      Feature(delPhoneCol, s"${delPhoneCol}_$frequency", s"${delPhoneCol}_$similarity", DEL_PHONE_THRESHOLD, s"deliveryPhone"),
    addressCol ->
      Feature(addressCol, s"${addressCol}_$frequency", s"${addressCol}_$similarity", ADDRESS_THRESHOLD, s"deliveyAddress"),
    emailCol ->
      Feature(emailCol, s"${emailCol}_$frequency", s"${emailCol}_$similarity", EMAIL_THRESHOLD, "deliveryEmail"),
    bankAccCol ->
      Feature(bankAccCol, s"${bankAccCol}_$frequency", s"${bankAccCol}_$similarity", BANK_ACCOUNT_THRESHOLD, s"bankAccount"),
    deviceCol ->
      Feature(deviceCol, s"${deviceCol}_$frequency", s"${deviceCol}_$similarity", DEVICE_THRESHOLD, s"device"),
    ruContactCol ->
      Feature(ruContactCol, s"${ruContactCol}_$frequency", s"${ruContactCol}_$similarity", RU_CONTACT_THRESHOLD, s"ruBeneficiary"),
    merchantBankCol ->
      Feature(merchantBankCol, s"${merchantBankCol}_$frequency", s"${merchantBankCol}_$similarity", MERCHANT_BANK_THRESHOLD, s"merchantAccount")
  )

  val profileColumnsMap = Map(
    signUpDate -> UDF.dateSimilarity(col(s"${signUpDate}_1"), col(s"${signUpDate}_2")).as(s"${signUpDate}_$similarity"),
    emailVerified -> UDF.boolSimilarity(col(s"${emailVerified}_1"), col(s"${emailVerified}_2")).as(s"${emailVerified}_$similarity"),
    gender -> UDF.boolSimilarity(col(s"${gender}_1"), col(s"${gender}_2")).as(s"${gender}_$similarity"),
    age -> UDF.boolSimilarity(col(s"${age}_1"), col(s"${age}_2")).as(s"${age}_$similarity"),
    kyc -> UDF.boolSimilarity(col(s"${kyc}_1"), col(s"${kyc}_2")).as(s"${kyc}_$similarity"),
    balance -> UDF.GMSimilarity(col(s"${balance}_1"), col(s"${balance}_2")).as(s"${balance}_$similarity"),
    appLanguage -> UDF.boolSimilarity(col(s"${appLanguage}_1"), col(s"${appLanguage}_2")).as(s"${appLanguage}_$similarity")
  )

  val compulsoryFeature = Seq(col(s"${appLanguage}_$similarity") === 1)

  class ModelConfigClass() {

    val rfParamMap: RandomForestClassifier => ParamMap = (model: RandomForestClassifier) => ParamMap(
      model.featuresCol -> modelFeaturesCol,
      model.labelCol -> initialLabelCol,
      model.probabilityCol -> probabCol,
      model.predictionCol -> initialPredCol,
      model.rawPredictionCol -> "rawPrediction_1",
      model.impurity -> "entropy",
      model.featureSubsetStrategy -> "auto",
      model.maxDepth -> 10,
      model.numTrees -> 200,
      model.subsamplingRate -> 0.5
    )

    val dtParamMap: DecisionTreeClassifier => ParamMap = (model: DecisionTreeClassifier) => ParamMap(
      model.featuresCol -> modelFeaturesCol,
      model.labelCol -> confidentLabelCol,
      model.probabilityCol -> "probability_2",
      model.predictionCol -> similarCol,
      model.rawPredictionCol -> "rawPrediction_2",
      model.impurity -> "entropy",
      model.maxDepth -> 10
    )

  }

}
