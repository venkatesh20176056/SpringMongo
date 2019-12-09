package com.paytm.map.features.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object ArgsUtils {

  // all jobs need a target day
  val TARGET_DATE_INDEX = 0

  val FormatPattern = "yyyy-MM-dd"
  val formatter = DateTimeFormat.forPattern(FormatPattern)

  def getTargetDate(args: Array[String]): DateTime = {
    val targetDay: String = args(TARGET_DATE_INDEX)
    formatter.parseDateTime(targetDay)
  }

  def getTargetDateStr(args: Array[String]): String = {
    args(TARGET_DATE_INDEX)
  }

  def isTargetDateValid(args: Array[String]): Boolean = {
    ArgsUtils.getTargetDateStr(args).matches("\\d{4}-\\d{2}-\\d{2}")
  }
}
