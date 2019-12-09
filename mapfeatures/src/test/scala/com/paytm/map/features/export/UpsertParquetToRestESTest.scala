package com.paytm.map.features.export

import java.net.InetAddress

import scala.collection.JavaConversions._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}
import java.util.concurrent.TimeUnit.MINUTES

import com.paytm.map.features.export.elasticsearch._
import com.paytm.map.features.export.elasticsearch.{EsDataFrameMappingConf, EsDataFrameWriteConf}
import org.apache.http.HttpHost
import org.apache.spark.sql.DataFrame
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.util.Random

class UpsertParquetToRestESTest extends FunSpec with Matchers with DataFrameSuiteBase with BeforeAndAfterAll with BeforeAndAfter {

  // ES and Spark both set this properties so it needs to be disabled from ES
  System.setProperty("es.set.netty.runtime.available.processors", "false")

  val embeddedElastic = EmbeddedElastic.builder()
    .withElasticVersion("6.0.0")
    .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
    .withSetting(PopularProperties.CLUSTER_NAME, "my_cluster")
    .withEsJavaOpts("-Xms128m -Xmx512m")
    .withStartTimeout(1, MINUTES)
    .build()

  override def beforeAll {
    super.beforeAll()
    embeddedElastic.start()
  }

  before {
    embeddedElastic.deleteIndices()
  }

  override def afterAll {
    super.afterAll()
    embeddedElastic.stop()
  }

  def createClient() = {
    val settings = Settings.builder().put("cluster.name", "my_cluster").build()
    new PreBuiltTransportClient(settings)
      .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9350))
  }

  def upsertToES(upsertData: DataFrame, indexName: String = "test_index"): Unit = {
    val esClientConf = EsRestClientConf(
      httpHosts  = Seq(new HttpHost("localhost", 9200, "http")),
      numBuckets = 1
    )

    val esMappingConf = EsDataFrameMappingConf(
      esMappingId = Option("a_key")
    )

    val esWriteConf = EsDataFrameWriteConf(
      bulkActions           = 20,
      bulkSizeInMB          = 1,
      concurrentRequests    = 2,
      flushTimeoutInSeconds = 10
    )

    upsertData.restUpsertToEs(
      esIndex          = indexName,
      esType           = "test_type",
      changeColumnName = "change",
      clientConf       = esClientConf,
      mappingConf      = esMappingConf,
      writeConf        = esWriteConf
    )

    embeddedElastic.refreshIndices()
  }

  describe("Upsert to ES") {

    it("should upsert the document to ES (insert test)") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val parquetData = Seq((1, 1.0, Seq.empty[String]), (2, 2.0, Seq.empty[String]))
        .toDF("a_key", "a_number", "change")

      val baseIndex = Random.alphanumeric.take(10).mkString.toLowerCase
      val index = baseIndex + "_*"
      upsertToES(parquetData, baseIndex)

      val client = createClient()
      client.prepareMultiSearch()
      val hits = client.prepareSearch(index)
        .setTypes("test_type")
        .execute().actionGet().getHits
      hits.totalHits should equal(2)

      val actual = hits.map { h =>
        val fields = h.getSourceAsMap
        (fields("a_key"), fields("a_number"))
      }.toSet
      val expect = Set((1, 1.0), (2, 2.0))

      actual should equal(expect)
    }

    it("should upsert the document to ES (update test)") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val parquetData = Seq((1, 1.0, Seq.empty[String]), (2, 2.0, Seq.empty[String]))
        .toDF("a_key", "a_number", "change")

      val baseIndex = Random.alphanumeric.take(10).mkString.toLowerCase
      val index = baseIndex + "_*"
      upsertToES(parquetData, baseIndex)

      val updates = Seq((1, 2.0, Seq("a_number")), (2, 3.0, Seq("a_number")))
        .toDF("a_key", "a_number", "change")

      upsertToES(updates, baseIndex)

      val client = createClient()
      val hits = client.prepareSearch(index)
        .setTypes("test_type")
        .execute().actionGet().getHits
      hits.totalHits should equal(2)

      val actual = hits.map { h =>
        val fields = h.getSourceAsMap
        (fields("a_key"), fields("a_number"))
      }.toSet
      val expect = Set((1, 2.0), (2, 3.0))

      actual should equal(expect)
    }

    it("should insert the document to ES") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val parquetData = Seq((1, 1.0, Seq[String]()), (2, 2.0, Seq[String]()))
        .toDF("a_key", "a_number", "change")

      val baseIndex = Random.alphanumeric.take(10).mkString.toLowerCase
      val index = baseIndex + "_*"
      upsertToES(parquetData, baseIndex)

      val client = createClient()
      val hits = client.prepareSearch(index)
        .setTypes("test_type")
        .execute().actionGet().getHits
      hits.totalHits should equal(2)

      val actual = hits.map { h =>
        val fields = h.getSourceAsMap
        (fields("a_key"), fields("a_number"))
      }.toSet
      val expect = Set((1, 1.0), (2, 2.0))

      actual should equal(expect)
    }

    it("should insert one document to ES even after double inserts") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val baseIndex = Random.alphanumeric.take(10).mkString.toLowerCase()
      val index = baseIndex + "_*"
      //first
      val parquetData1 = Seq((1, 1.0, Seq[String]()), (2, 2.0, Seq[String]()))
        .toDF("a_key", "a_number", "change")
      upsertToES(parquetData1, baseIndex)
      //second
      val parquetData2 = Seq((1, 2.0, Seq[String]()), (2, 3.0, Seq[String]()))
        .toDF("a_key", "a_number", "change")

      upsertToES(parquetData2, baseIndex)

      val client = createClient()
      val hits = client.prepareSearch(index)
        .setTypes("test_type")
        .execute().actionGet().getHits
      hits.totalHits should equal(2)

      val actual = hits.map { h =>
        val fields = h.getSourceAsMap
        (fields("a_key"), fields("a_number"))
      }.toSet
      val expect = Set((1, 2.0), ((2, 3.0)))

      actual should equal(expect)
    }

    it("should upsert sequentially fields of a document to ES (insert test)") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val letterData = Seq((1, "b", Seq[String]()), (2, "c", Seq[String]()))
        .toDF("a_key", "a_letter", "change")

      val numberData = Seq((1, 1.0, Seq[String]()))
        .toDF("a_key", "a_number", "change")

      val baseIndex = Random.alphanumeric.take(10).mkString.toLowerCase
      val index = baseIndex + "_*"
      upsertToES(letterData, baseIndex)
      upsertToES(numberData, baseIndex)

      val client = createClient()
      val hits = client.prepareSearch(index)
        .setTypes("test_type")
        .execute().actionGet().getHits
      hits.totalHits should equal(2)

      val actual = hits.map { h =>
        val fields = h.getSourceAsMap
        (fields("a_key"), fields.getOrElse("a_letter", null), fields.getOrElse("a_number", null))
      }.toSet
      val expect = Set((1, null, 1.0), (2, "c", null))

      actual should equal(expect)
    }
  }
}
