package com.paytm.map.features

import org.apache.spark.sql.SparkSession

import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.utils.ConvenientFrame._
import org.apache.spark.sql.types.StructType

sealed trait SparkJobValidation {

  def &&(sparkValidation: SparkJobValidation): SparkJobValidation = this match {
    case SparkJobValid => sparkValidation
    case x             => x
  }

}

case object SparkJobValid extends SparkJobValidation
case class SparkJobInvalid(reason: String) extends SparkJobValidation

object SparkJobValidations {

  object DailyJobValidation {
    val DateIndex = 0

    def validate(sparkSession: SparkSession, args: Array[String]): SparkJobValidation = {
      if (!ArgsUtils.isTargetDateValid(args)) {
        SparkJobInvalid(s"Date must be in form 'yyyy-MM-dd', received `${args(DateIndex)}`")
      } else SparkJobValid
    }

  }

  object ArgLengthValidation {
    def validate(args: Array[String], expectedLength: Int): SparkJobValidation = {
      if (args.length == expectedLength) SparkJobValid
      else SparkJobInvalid("| Expected ${expectedLength} arguments}, but got ${args.length} arguments.")
    }
  }

  object SchemaValidation {

    def validate(sparkSession: SparkSession, inputPath: Seq[String],
      expectedSchema: StructType): SparkJobValidation = {

      val inputDF = sparkSession.read.parquet(inputPath: _*)

      if (!inputDF.matchSchemaSubset(expectedSchema)) {
        SparkJobInvalid(
          s"""
             |Expected schema should be a subset of the input schema.
             |Expected schema: ${expectedSchema.treeString}
             |Input schema:    ${inputDF.schema.treeString}
           """.stripMargin
        )
      } else SparkJobValid
    }

  }
}