package com.paytm.map.featuresplus

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object StringUtils {

  val nameVariations: Map[String, String] = Map(
    "aa" -> "a",
    "ee" -> "i",
    "ii" -> "i",
    "oo" -> "u",
    "uu" -> "u",
    "kh" -> "k",
    "gh" -> "g",
    "jh" -> "j",
    "th" -> "t",
    "bh" -> "b",
    "sh" -> "s"
  )

  val keepOnlyFirstWord: UserDefinedFunction = udf((firstName: String) =>
    if (firstName.nonEmpty) firstName.split(" ").head
    else firstName)

  val dedupeCommonVariations: UserDefinedFunction = udf { (firstName: String) =>
    if (firstName.nonEmpty) {
      nameVariations.foldLeft(firstName) {
        case (a, b) => a.replaceAll(b._1, b._2)
      }
    } else firstName
  }

  val isSingleAlphabets: UserDefinedFunction = udf((name: String) =>
    if (name.length == 1) false else true)

  val replaceLastAlphabet: UserDefinedFunction = udf((name: String, from: String, to: String) =>
    if (name.takeRight(1) == from) name.dropRight(1) + to else name)

  val isNoVowelName: UserDefinedFunction = udf { (name: String) =>
    val vowelsInName = name.filter("aeiou".contains(_))
    if (vowelsInName.nonEmpty) true else false
  }

  val isNonAlphabets: UserDefinedFunction = udf((name: String) =>
    name.forall(_.isLetter))

}