package com.alibaba.graphar

import com.alibaba.graphar.reader.{VertexReader, EdgeReader}

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class SourceSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[2]")
    .appName("example")
    .getOrCreate()

  test("read simple csv") {
    val simpleCsvDf = spark.read
      .format("com.alibaba.graphar.source.simplecsv")
      .load("gar-test/ldbc_sample/csv/vertex/person/id/")

    simpleCsvDf.printSchema()
    simpleCsvDf.show()
    println(
      "number of partitions in simple csv source is " + simpleCsvDf.rdd.getNumPartitions)
    simpleCsvDf.write.mode("overwrite").format("csv").save("/tmp/vertex_id_output")
  }
 }