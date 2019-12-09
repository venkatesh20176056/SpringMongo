package com.paytm.map.features.utils.athena

import com.amazonaws.services.athena.AmazonAthena
import com.amazonaws.services.athena.model._

import scala.util.{Failure, Success, Try}

case class AthenaQueryExecutor(client: AmazonAthena) {

  def executeSync(database: String, outputLocation: String, query: String) =
    loopTillReady(this.submit(database, outputLocation, query))

  def submit(database: String, outputBucket: String, query: String): String = {
    val queryExecutionContext = new QueryExecutionContext().withDatabase(database)

    val resultConfiguration = new ResultConfiguration().withOutputLocation(outputBucket)
    val startQueryExecutionRequest = new StartQueryExecutionRequest()
      .withQueryString(query)
      .withQueryExecutionContext(queryExecutionContext)
      .withResultConfiguration(resultConfiguration)

    val startQueryExecutionResult = client.startQueryExecution(startQueryExecutionRequest)
    val executionId = startQueryExecutionResult.getQueryExecutionId
    executionId
  }

  def loopTillReady(executionId: String, pollInterval: Long = 1000): Try[Boolean] = {
    def loop(): Try[Boolean] = {
      val executionRequest = new GetQueryExecutionRequest().withQueryExecutionId(executionId)
      val queryExecution = client.getQueryExecution(executionRequest).getQueryExecution
      queryExecution.getStatus.getState match {
        case "FAILED" =>
          Failure(
            new Exception("Query Failed to run with Error Message: " + queryExecution.getStatus.getStateChangeReason)
          )
        case "CANCELLED" => Failure(new Exception("Query was cancelled."))
        case "SUCCEEDED" => Success(true)
        case _ =>
          Thread.sleep(pollInterval)
          loop()
      }
    }

    loop()
  }

  def dropTable(database: String, tableName: String, outputLocation: String) = {
    val query = s"DROP TABLE IF EXISTS $database.$tableName"
    executeSync(database, outputLocation, query)
  }

  def createParquetTable(database: String, tableName: String, schema: Map[String, String], dataLocation: String, outputLocation: String) = {
    val query =
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS `$database`.`$tableName`(
         |${schema.map { case (k, v) => s"`$k` $v" }.mkString(",\n")}
         |)
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
         |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
         |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         |LOCATION '$dataLocation'
         |TBLPROPERTIES ('parquet.compress'='SNAPPY')
       """.stripMargin
    executeSync(database, outputLocation, query)
  }
}
