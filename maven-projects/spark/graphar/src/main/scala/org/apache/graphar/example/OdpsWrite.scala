package org.apache.graphar

import org.apache.graphar.{AdjListType, GeneralParams}
import org.apache.graphar.writer.{VertexWriter, EdgeWriter}
import org.apache.graphar.util.IndexGenerator

import java.net.URI
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.LoaderOptions

object OdpsWrite {
  def Df2GraphAr(spark: SparkSession,
                 vertexTableName: String,
                 edgeTableName: String,
                 vertexInfoPath: String,
                 edgeInfoPath: String,
                 ossOutputPath: String,
                 fileFormat: String,
                 vertexCol: String,
                 sourceVertexCol: String,
                 destVertexCol: String): Unit = {
    // val vertexDf = spark.read.option("delimiter", "|").option("header", "true").csv(vertexTableName)
    val vertexDf = spark.table(vertexTableName)
    vertexDf.persist(
      GeneralParams.defaultStorageLevel
    ) // cache the vertex DataFrame
    val vertexPath = new fs.Path(vertexInfoPath)
    // val edgeDf = spark.read.option("delimiter", "|").option("header", "true").csv(edgeTableName)
    val edgeDf = spark.table(edgeTableName)
    edgeDf.persist(
      GeneralParams.defaultStorageLevel
    ) // cache the edge DataFrame
    val fileSystem1 = fs.FileSystem.get(vertexPath.toUri(), spark.sparkContext.hadoopConfiguration)
    val vertexInput = fileSystem1.open(vertexPath)
    val vertexYaml = new Yaml(new Constructor(classOf[VertexInfo], new LoaderOptions()))
    val vertexInfo = vertexYaml.load(vertexInput).asInstanceOf[VertexInfo]
    val df_and_mapping = IndexGenerator.generateVertexIndexColumnAndIndexMapping(vertexDf, vertexCol)
    df_and_mapping._1.persist(
        GeneralParams.defaultStorageLevel
    ) // cache the vertex DataFrame with index
    df_and_mapping._2.persist(
      GeneralParams.defaultStorageLevel
    ) // cache the index mapping DataFrame
    // val index_mapping = IndexGenerator.constructVertexIndexMapping(vertexDf, vertexCol)
    vertexDf.unpersist()
    val df_with_index = df_and_mapping._1
    val index_mapping = df_and_mapping._2
    val vertexWriter = new VertexWriter(ossOutputPath, vertexInfo, df_with_index)
    val vertexNum = vertexWriter.getVertexNum()
    vertexWriter.writeVertexProperties()
    df_with_index.unpersist()
    
    val infoPath = new fs.Path(edgeInfoPath)
    val fileSystem2 = fs.FileSystem.get(infoPath.toUri(), spark.sparkContext.hadoopConfiguration)
    val input = fileSystem2.open(infoPath)
    val edgeYaml = new Yaml(new Constructor(classOf[EdgeInfo], new LoaderOptions()))
    val edgeInfo = edgeYaml.load(input).asInstanceOf[EdgeInfo]
    val edgeDfWithIndex = IndexGenerator.generateSrcAndDstIndexForEdgesFromMapping(edgeDf, index_mapping, index_mapping)
    edgeDfWithIndex.persist(
      GeneralParams.defaultStorageLevel
    ) // cache the edge DataFrame with index
    edgeDf.unpersist()
    val adjListType = AdjListType.unordered_by_source
    val writer = new EdgeWriter(ossOutputPath, edgeInfo, adjListType, vertexNum, edgeDfWithIndex)
    // val writer = new EdgeWriter(ossOutputPath, edgeInfo, adjListType, 13259554849L, edgeDfWithIndex)
    writer.writeAdjList()
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    // sparkConf
    //   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //   .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .appName("OdpsWrite")
      .config(sparkConf)
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    // table name
    val vertexTableName: String = args(0)
    val edgeTableName: String = args(1)
    val vertexInfoPath: String = args(2)
    val edgeInfoPath: String = args(3)
    val ossOutputPath: String = args(4)
    val vertexCol: String = args(5)
    val sourceVertexCol: String = args(6)
    val destVertexCol: String = args(7)
    val fileFormat: String = args(8)

    val sc = spark.sparkContext
    import spark._
    import sqlContext.implicits._
    Df2GraphAr(spark, vertexTableName, edgeTableName, vertexInfoPath, edgeInfoPath, ossOutputPath, fileFormat,
               vertexCol, sourceVertexCol, destVertexCol)
    spark.close()
  }
}