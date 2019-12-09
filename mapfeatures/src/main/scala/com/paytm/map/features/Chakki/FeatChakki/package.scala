package com.paytm.map.features.Chakki

import org.apache.spark.sql.Column

package object FeatChakki {

  // UDF_T Type
  sealed trait UDF_T {
    override def toString: String = this.getClass.getSimpleName.stripSuffix("$")
  }

  object PRE_UDF extends UDF_T

  object POST_UDF extends UDF_T

  ///////////////
  //Parsed FeatureType
  sealed trait ParsedFeat {
    val feats: Seq[Column]
    val dependency: Seq[String]
  }

  case class UDFParsed(feats: Seq[Column], dependency: Seq[String]) extends ParsedFeat

  case class UDAFParsed(feats: Seq[Column], dependency: Seq[String]) extends ParsedFeat

  case class UDWFParsed(feats: Seq[Column], udaf: Seq[Column], dependency: Seq[String]) extends ParsedFeat

}
