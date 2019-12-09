package com.paytm.map.features.sanity

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.utils.{ArgsUtils, FileUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import org.apache.commons.io.Charsets
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object CheckFields extends CheckFieldsJob with SparkJob with SparkJobBootstrap {
}

trait CheckFieldsJob {
  this: SparkJob =>

  val JobName = "CheckFields"

  val fieldsDictionary = Map(
    "Integer" -> Seq(IntegerType, LongType),
    "String" -> Seq(StringType),
    "Long" -> Seq(LongType),
    "Double" -> Seq(DoubleType, FloatType),
    "Date" -> Seq(DateType, TimestampType),
    "DateTime" -> Seq(TimestampType)
  )

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {

    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val groupId = args(4)

    /** Inputs */
    val featuresInputPath = s"${args(1)}/dt=$targetDateStr"
    val baseInputCsvPath = args(2)
    val latestCsvInputDate: Option[String] = FileUtils.getLatestTableDate(baseInputCsvPath, spark, targetDateStr, "updated_at=", "")
    val inputCsvPath = latestCsvInputDate.map(date => s"$baseInputCsvPath/updated_at=$date/feature_list_india.csv").getOrElse("feature_list_india_missing")

    /** Output */
    val outputPath = s"${args(3)}/dt=$targetDateStr/group${groupId}SanityFieldCheck.csv"

    println(s"Reading data from featuresInputPath = $featuresInputPath, csvPath = $inputCsvPath and writing result to ${outputPath}")
    val dataSchema: Array[StructField] = spark.read
      .parquet(featuresInputPath)
      .schema.fields

    val nonNestedFieldNames: Array[String] = dataSchema.map(_.name)

    val nestedFieldNames: Array[Option[String]] = dataSchema.flatMap(field => {
      if (field.dataType.isInstanceOf[ArrayType] && field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]) {
        field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.map(nestedField => {
          Some(s"${field.name}.${nestedField.name}")
        })
      } else {
        None
      }
    })

    val allFieldsFromFeaturesData = nonNestedFieldNames ++ nestedFieldNames.flatten[String]

    val csvDF: Array[Row] = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputCsvPath)
      .select("Name", "Datatype")
      .collect()

    val csvData: Array[(String, String)] = csvDF.map(row => (row.getAs[String]("Name"), row.getAs[String]("Datatype")))
    val csvFields: Array[String] = csvData.map(data => data._1)

    val notInDataMsg = checkFields(csvFields, allFieldsFromFeaturesData, "DATA")
    val notInCsvMsg = checkFields(allFieldsFromFeaturesData, csvFields, "CSV")

    val csvDataFiltered: Array[(String, String)] = for {
      aCsvItem <- csvData
      if allFieldsFromFeaturesData contains aCsvItem._1
    } yield aCsvItem
    val dataTypeMsg = checkDataType(csvDataFiltered, dataSchema)

    val emailMsg = getHTML(notInDataMsg, notInCsvMsg, dataTypeMsg)

    //Write the email content to s3
    val dest = new Path(outputPath)
    val fs = FileUtils.fs(dest, spark)
    val out = fs.create(dest, true)
    out.write(emailMsg.getBytes(Charsets.UTF_8))
    out.close()
    println(s"Finished writing file to ${outputPath}")
  }

  def checkDataType(data: Array[(String, String)], dataSchema: Array[StructField]) = {
    val dataSchemaMap: Map[String, DataType] = (for (d <- dataSchema) yield (d.name, d.dataType)).toMap

    val nestedSchema: Array[(String, DataType)] = for {
      field <- dataSchema
      if field.dataType.isInstanceOf[ArrayType]
      if field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      nestedField <- field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields
    } yield (s"$field.$nestedField", nestedField.dataType)

    val finalMap = dataSchemaMap ++ nestedSchema.toMap

    val nonMatchingFields = scala.collection.mutable.ArrayBuffer.empty[Seq[String]]

    for {
      (name, featureServiceType) <- data
      field <- finalMap.get(name)
      csvDataTypes <- fieldsDictionary.get(featureServiceType)
      matchingTypes = for (csvDataType <- csvDataTypes if compareTypes(field, csvDataType, featureServiceType)) yield csvDataType
      if (matchingTypes.size == 0)
    } nonMatchingFields += (Seq(name, featureServiceType, field.toString))

    if (nonMatchingFields.size > 0) {
      s"""
      <h2>DataType Mismatch Fields</h2>
      <p>List of fields for which datatype mismatches in CSV and Generated Data. Please Investigate</p>
      <table id="customers">
      <tr>
          <th>Name</th>
          <th>CSV-Type</th>
          <th>DATA-Type</th>
      </tr>
      ${getHTMLTable(nonMatchingFields)}
      </table>
      """
    } else
      "<h2>No DataType Mismatch Fields</h2>"
  }

  def compareTypes(dataSchema: DataType, csvDataType: DataType, featureServiceType: String) = {
    if (!dataSchema.isInstanceOf[ArrayType]) {
      dataSchema == csvDataType
    } else if (dataSchema.isInstanceOf[ArrayType] && featureServiceType == "List[String]") {
      dataSchema.asInstanceOf[ArrayType].elementType.isInstanceOf[StringType]
    } else if (dataSchema.isInstanceOf[ArrayType] && featureServiceType == "Nested") {
      dataSchema.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
    } else {
      false
    }
  }

  def checkFields(csvFields: Array[String], allFieldsFromFeaturesData: Array[String], nameToken: String = "DATA") = {
    val diffFields = csvFields.toSet.diff(allFieldsFromFeaturesData.toSet).toSeq
    if (diffFields.length > 0) {
      s"""
        <h2>Fields Not found in $nameToken</h2>
        <p>List of missing fields in $nameToken. Please Investigate accordingly</p>
        <table id="customers">
        ${getHTMLTable(grouped(diffFields, 3))}
        </table>
        """
    } else {
      s"<h2>All Fields found in $nameToken</h2>"
    }
  }

  def getHTMLTable(diffFields: Seq[Seq[String]]) = {
    val tableItems = diffFields.map(row => {
      row.map(item => s"<td>$item</td>")
    })
    val tableRows = tableItems.map(row => {
      s"<tr>\n${row.mkString("\n")}\n</tr>"
    })
    tableRows.mkString("\n")
  }

  def grouped(dataList: Seq[String], stepSize: Int) = {
    dataList.sliding(stepSize, stepSize).toSeq
  }
  def getHTML(notInDataMsg: String, notInCsvMsg: String, dataTypeMsg: String) = {
    val html = """
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    #customers {
        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
        border-collapse: collapse;
        width: 100%;
    }

    #customers td, #customers th {
        border: 1px solid #ddd;
            padding: 8px;
    }

    #customers tr:nth-child(even){background-color: #f2f2f2;}

    #customers tr:hover {background-color: #ddd;}

    #customers th {
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: left;
        background-color: #4CAF50;
        color: white;
    }
    </style>
    </head>
    <body>
    <div>
    """ +
      notInDataMsg + "\n" +
      """
     </div>
     <div>
     """ +
      notInCsvMsg + "\n" +
      """
    </div>
     <div>
      """ +
      dataTypeMsg + "\n" +
      """
     </div>
     </body>
    </html>
    """
    html
  }
}

