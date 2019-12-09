package com.paytm.map.featuresplus.superuserid

import com.paytm.map.features.SparkJobValidations.DailyJobValidation
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.paytm.map.features.utils.{ArgsUtils, Settings}
import com.paytm.map.features.{SparkJob, SparkJobBootstrap, SparkJobValidation}
import com.paytm.map.featuresplus.superuserid.Constants._
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

object GraphComputation extends GraphComputationJob with SparkJobBootstrap with SparkJob

trait GraphComputationJob {
  this: SparkJob =>

  val JobName = "GraphComputeEngine"

  /**
   * This files receives all relations and similarityValue from the model ;
   * does another round of thresholding for degree/cluster size/ etc to remove open users
   * Finally computes connected components
   */

  def validate(spark: SparkSession, settings: Settings, args: Array[String]): SparkJobValidation = {
    DailyJobValidation.validate(spark, args)
  }

  def runJob(spark: SparkSession, settings: Settings, args: Array[String]): Unit = {
    import spark.implicits._

    /** arguments **/
    val date = ArgsUtils.getTargetDate(args)
    val dateStr = ArgsUtils.getTargetDateStr(args)
    val Path = new PathsClass(settings.featuresDfs.featuresPlusRoot)

    val allRelations = spark.read.parquet(s"${Path.modelledRelationPath}/dt=$dateStr").
      select(col(s"${customerIdCol}_1").cast(LongType), col(s"${customerIdCol}_2").cast(LongType), col(similarCol)).
      withColumn(s"${inverseCustIdCol}_1", lit(LONG_MAX) - col(s"${customerIdCol}_1")).
      withColumn(s"${inverseCustIdCol}_2", lit(LONG_MAX) - col(s"${customerIdCol}_2")).
      select(s"${inverseCustIdCol}_1", s"${inverseCustIdCol}_2", similarCol)

    val vertices: RDD[(VertexId, VertexAttr)] = Seq(s"${inverseCustIdCol}_1", s"${inverseCustIdCol}_2").map { i =>
      allRelations.select(col(i)).distinct()
    }.reduce(_ union _).distinct.rdd.
      map(r => (r.getLong(0), VertexAttr(vertex_id = r.getLong(0), customer_id = LONG_MAX - r.getLong(0), super_user_id = LONG_MAX - r.getLong(0), degree = 0L, cluster_size = 0L)))

    val edges: RDD[Edge[EdgeAttr]] = allRelations.
      select(s"${inverseCustIdCol}_1", s"${inverseCustIdCol}_2", similarCol).rdd.
      map {
        case Row(id1: Long, id2: Long, similarValue: Double) =>
          Edge(id1, id2, EdgeAttr(similarValue))
      }

    val initialGraph = Graph(vertices, edges)

    val vertexDegree: VertexRDD[Int] = initialGraph.degrees
    val degreeGraph = initialGraph.joinVertices(vertexDegree) {
      (id, attr, newAttr) => attr.copy(degree = newAttr)
    }

    val degreeFilteredGraph = degreeGraph.subgraph(vpred = (id: VertexId, attr: VertexAttr) => attr.degree < OPEN_SYSTEM_THRESHOLD)

    val connectUsers: Graph[VertexId, EdgeAttr] = degreeFilteredGraph.connectedComponents

    connectUsers.cache()

    val size = connectUsers.vertices.toDF("id", "clusterId").
      groupBy("clusterId").
      agg(count("id").as("size"))

    val verticesClusterSize: RDD[(VertexId, (Long, Long))] = connectUsers.vertices.toDF("id", "clusterId").join(size, Seq("clusterId")).
      select("id", "clusterId", "size").
      filter($"size" lt CLUSTER_SIZE_THRESHOLD).
      rdd.
      map { case Row(vertexId: Long, clusterId: Long, size: Long) => (vertexId, (clusterId, size)) }.sortBy(_._1)

    val superUsersGraph = degreeFilteredGraph.joinVertices(verticesClusterSize) {
      (id, oldAttr, newAttr) => oldAttr.copy(super_user_id = LONG_MAX - newAttr._1, cluster_size = newAttr._2)
    }

    val nonPhishyGraph = superUsersGraph.
      subgraph(vpred = (id: VertexId, attr: VertexAttr) => attr.cluster_size < CLUSTER_SIZE_THRESHOLD)

    val nonPhishyDF = nonPhishyGraph.vertices.
      toDF("vertex_id", "attr").
      select($"attr.vertex_id", $"attr.customer_id", $"attr.super_user_id", $"attr.degree", $"attr.cluster_size")

    val toExport = {
      nonPhishyDF.write.mode(SaveMode.Overwrite).parquet(s"${Path.exportPath}/dt=$dateStr")
      spark.read.parquet(s"${Path.exportPath}/dt=$dateStr").select(customerIdCol, superIdCol).
        withColumn(sameSuperId, when(col(customerIdCol) === col(superIdCol), 1).otherwise(0))
    }

    toExport.write.mode(SaveMode.Overwrite).parquet(s"${Path.finalOutputPath}/dt=$dateStr")

  }

}