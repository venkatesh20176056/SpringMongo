package com.paytm.map.features.utils

import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpGet
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source.fromInputStream
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object CMACatMapping {

  case class Mapping(
    t4Id: String,
    sourceId: Int,
    newMapCategoryId: Int
  )

  case class NameMapping(
    id: Int,
    name: String
  )

  def getT4Mappings(spark: SparkSession): DataFrame = {
    import spark.implicits._
    implicit val formats = DefaultFormats

    val client = HttpClientBuilder.create.build

    //Move this to config
    val response = client.execute(new HttpGet("http://internal-map-cma-production-lb-1488664899.ap-south-1.elb.amazonaws.com/clients/paytm-india/t4Mappings"))

    val responseCode = response.getStatusLine.getStatusCode
    if (responseCode == 200) {
      val inputStream = response.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      JsonMethods.parse(content).extract[List[Mapping]].toDF
        .filter($"sourceid" === 1)
        .select($"t4id" as "category_id", $"newmapcategoryid" as "map_category_id")
    } else {
      val message = s"Error in getting t4 Mappings from cma with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

  def getCategoryNameMappings(spark: SparkSession): DataFrame = {
    import spark.implicits._

    implicit val formats = DefaultFormats

    val client = HttpClientBuilder.create.build

    //Move this to config
    val response = client.execute(new HttpGet("http://internal-map-cma-production-lb-1488664899.ap-south-1.elb.amazonaws.com/clients/paytm-india/categories2/flat"))

    val responseCode = response.getStatusLine.getStatusCode

    if (responseCode == 200) {
      val inputStream = response.getEntity.getContent
      val content = fromInputStream(inputStream).getLines.mkString
      JsonMethods.parse(content).extract[List[NameMapping]].toDF
    } else {
      val message = s"Error in getting engage category name mappings from cma with REST response code: $responseCode."
      throw new RuntimeException(message)
    }
  }

}
