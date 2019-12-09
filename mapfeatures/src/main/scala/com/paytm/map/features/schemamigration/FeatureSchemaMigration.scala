package com.paytm.map.features.schemamigration

import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import com.paytm.map.features.base.BaseTableUtils.{LMSAccount, OperatorDueDate, PromoUsage}
import com.paytm.map.features.utils.athena.{AthenaClientFactory, AthenaQueryExecutor}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

object FeatureSchemaMigration extends FeatureSchemaMigrationJob
  with SparkJob with SparkJobBootstrap

final case class ColumnWithSchema(name: String, dataType: String)

trait FeatureSchemaMigrationJob {
  this: SparkJob =>

  val JobName = "FeatureSchemaMigration"

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._
    val targetDateStr = ArgsUtils.getTargetDate(args).toString(ArgsUtils.formatter)
    val bucketName = settings.midgarS3Bucket
    val env = settings.environment

    val numericUdf = udf { column: String => column.forall(_.isDigit) }

    val executorCount = spark.conf.getOption("spark.executor.instances").getOrElse("50").toInt
    val coresPerExecutors = spark.conf.getOption("spark.executor.cores").getOrElse("4").toInt
    val partitions = (executorCount * coresPerExecutors) * 5

    val dataInPath = (level: Int) => s"s3a://$bucketName/prod/mapfeatures/features/$level/flatTable/dt=$targetDateStr"
    val dataOutPath = (level: Int) => s"s3a://$bucketName/$env/mapfeatures/athena/features/$level/flatTable/dt=$targetDateStr"

    val levels = Seq(1, 2, 3, 4)

    val schemas = levels.map { level =>
      val in = dataInPath(level)
      val out = dataOutPath(level)
      val data = spark.read.parquet(in).repartition(partitions).filter(numericUdf($"customer_id"))
      val schema = data.schema

      // Use this convention instead of writing the column names out in case they don't exist in a particular group
      val castToLongColumns = Seq("customer_id", "is_email_verified")
      val simpleDateColumns = schema.filter(_.name.endsWith("_date")).map(_.name)
      val operatorDueDateColumns = schema.filter(_.name.endsWith("_operator_due_dates")).map(_.name)
      val promoUsageDateColumns = schema.filter(_.name.endsWith("_promo_usage_dates")).map(_.name)
      val lmsAccountInfoColumns = schema.filter(_.name == "LMS_POSTPAID_account_details").map(_.name)

      val allDateColumns = data.schema.filter(_.dataType.sql.toLowerCase.contains("date")).map(_.name)
      val unsupportedDateColumns = allDateColumns.diff(
        simpleDateColumns ++ operatorDueDateColumns ++ promoUsageDateColumns ++ lmsAccountInfoColumns
      )

      if (unsupportedDateColumns.nonEmpty)
        throw new Exception("A new unsupported date column(s) has been added, please update the feature schema migration " +
          s"to support the following: ${unsupportedDateColumns.mkString("[", ",", "]")}")

      val castFn = castColumns(castToLongColumns, "bigint")
        .andThen(castColumns(simpleDateColumns, "string"))
        .andThen(castColumns(operatorDueDateColumns, "array<struct<" + castClassFields[OperatorDueDate] + ">>"))
        .andThen(castColumns(promoUsageDateColumns, "array<struct<" + castClassFields[PromoUsage] + ">>"))
        .andThen(castColumns(lmsAccountInfoColumns, "array<struct<" + castClassFields[LMSAccount] + ">>"))

      val castedData = castFn(data)

      castedData.write.mode(SaveMode.Overwrite).parquet(out)
      castedData.schema.map(s => ColumnWithSchema(s.name, s.dataType.sql.toLowerCase))
    }

    val queryExecutor = AthenaQueryExecutor(AthenaClientFactory.createClient)
    val newTables = updateAthenaTable(levels, schemas.head, dataOutPath, settings, queryExecutor)
    updateTableReferenceFile(newTables, levels, settings, spark, queryExecutor)
  }

  /**
   * Every time the job runs, it will create a new athena table for each level. Since the table schema can change
   * in unpredictable ways, it was decided to recreate the table everytime, instead of altering the existing table
   * and adding a new partition to it. This required the tables to be unique from each other, so a timestamp is appended
   * to the table name. Services interacting with these tables should refer to the table reference file to get the list
   * of the latest l1,l2,l3,l4 tables.
   */
  def updateAthenaTable(
    levels: Seq[Int],
    schema: Seq[ColumnWithSchema],
    dataPathFn: Int => String,
    settings: Settings,
    queryExecutor: AthenaQueryExecutor
  ): Seq[String] = {
    val database = "daily_flatfeatures"
    val bucketName = settings.midgarS3Bucket
    val env = settings.environment
    val outputPath = s"${settings.featuresDfs.schemaMigrationHistory}/tablechanges/".replaceFirst("s3a", "s3")

    levels.map { level =>
      val dataPath = dataPathFn(level).replaceFirst("s3a", "s3")
      val tableName = s"L${level}_${System.currentTimeMillis()}"
      queryExecutor.createParquetTable(database, tableName, schema.map(c => c.name -> c.dataType).toMap, dataPath, outputPath) match {
        case Failure(exception) => throw exception
        case Success(_)         => tableName
      }
    }
  }

  /**
   * Maintains the table reference file. This file will keep the last (backupToKeep + 1) successful migrations.
   * This function also drops the associated athena table when it gets removed from the reference file.
   *
   * @param backupsToKeep - the number of backups to keep, the latest row will be appended making the total backupsToKeep + 1
   */
  def updateTableReferenceFile(
    newTables: Seq[String],
    levels: Seq[Int],
    settings: Settings,
    spark: SparkSession,
    queryExecutor: AthenaQueryExecutor,
    backupsToKeep: Int = 2
  ): Unit = {
    import spark.implicits._
    val database = "daily_flatfeatures"
    val bucketName = settings.midgarS3Bucket
    val env = settings.environment
    val outputPath = s"${settings.featuresDfs.schemaMigrationHistory}/tablechanges/".replaceFirst("s3a", "s3")
    val tableNamesPath = s"${settings.featuresDfs.schemaMigrationHistory}/tablenames/"
    val columnNames = levels.map(l => s"L$l")

    val Seq(t0, t1, t2, t3) = newTables
    val newRow = Seq((t0, t1, t2, t3)).toDF(columnNames: _*)

    Try(spark.read.csv(tableNamesPath)) match {
      case Failure(_) =>
        // this means that there is no existing table reference file (most likely this is the first time the job has been run)
        newRow.coalesce(1).write.mode(SaveMode.Overwrite).csv(tableNamesPath)
      case Success(data) =>
        val schema = StructType(columnNames.map(name => StructField(name, DataTypes.StringType)))
        // This is never more then 3 rows, which each contain 4 small strings, so collect isn't an issue.
        val tableReferences = data.collect()
        val tableReferencesDf = dataFrameFromArray(tableReferences, schema, spark)
        newRow.union(tableReferencesDf.limit(backupsToKeep)).coalesce(1).write.mode(SaveMode.Overwrite).csv(tableNamesPath)

        val tablesToDrop = tableReferences.drop(backupsToKeep)
        tablesToDrop
          .foreach(row => row.toSeq.map(_.toString)
            .foreach(tableName => queryExecutor.dropTable(database, tableName, outputPath)))
    }

  }

  val castColumn = (name: String, castTo: String) => (df: DataFrame) => df.withColumn(name, col(name).cast(castTo))
  val castColumns = (names: Seq[String], castTo: String) => (df: DataFrame) => names.foldLeft(df)((d, c) => castColumn(c, castTo)(d))

  def castClassFields[T <: Product: TypeTag] = {
    def castDateColumns(s: String) = s match {
      case "date" => "string"
      case x      => x
    }

    Encoders.product[T].schema.map(a => a.name + ": " + castDateColumns(a.dataType.sql.toLowerCase)).mkString(", ")
  }
  private def dataFrameFromArray(arr: Array[Row], schema: StructType, spark: SparkSession): DataFrame =
    spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(arr), schema)
}
