package com.alibaba.graphar.source.simplecsv

import com.opencsv.CSVReader

import java.util
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException
import java.net.URISyntaxException
import java.net.URL
import java.util.Arrays
import java.util.Iterator
import java.util.List
import java.util.function.Function

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider{

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table ={
    val path = map.get("path")
    new CsvBatchTable(path)
  }

}

object SchemaUtils {
  def getSchema(path:String):StructType = {
    val firstLine: String = "id"
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }
}
/*
  Defines Read Support and Initial Schema
 */

class CsvBatchTable(path:String) extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = SchemaUtils.getSchema(path)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CsvScanBuilder(path)
}


/*
   Scan object with no mixins
 */
class CsvScanBuilder(path:String) extends ScanBuilder {
  override def build(): Scan = new CsvScan(path)
}


// simple class to organise the partition
case class CsvPartition(val partitionNumber:Int, path:String, header:Boolean=true) extends InputPartition


/*
    Batch Reading Support
    The schema is repeated here as it can change after column pruning etc
 */

class CsvScan(path:String) extends Scan with Batch{
  override def readSchema(): StructType = SchemaUtils.getSchema(path)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val partitions = ( 0 to  9).map(value => CsvPartition(value, path, false))
    partitions.toArray

  }
  override def createReaderFactory(): PartitionReaderFactory = new CsvPartitionReaderFactory()
}


// reader factory
class CsvPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new
      CsvPartitionReader(partition.asInstanceOf[CsvPartition])
}


// parathion reader
class CsvPartitionReader(inputPartition: CsvPartition) extends PartitionReader[InternalRow] {

  var iterator: Iterator[Array[String]] = null

  @transient
  def next = {
    if (iterator == null) {
      val resource = this.getClass.getClassLoader.getResource(inputPartition.path + "chunk" + inputPartition.partitionNumber.toString())
      val fileReader = new FileReader(new File(resource.toURI()))
      val csvReader = new CSVReader(fileReader)
      iterator = csvReader.iterator()
    }
    iterator.hasNext()
  }

  def get = {
    val values = iterator.next()
    InternalRow.fromSeq(values.map(value => UTF8String.fromString(value)))
  }

  def close() = Unit

}