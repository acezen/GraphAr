package com.alibaba.graphar.datasources

import com.alibaba.graphar.GeneralParams

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce._
import org.apache.spark.internal.Logging

object GarCommitProtocol {
  private def binarySearchPair(aggNums: List[Int], key: Int): (Int, Int) = {
    var low = 0
    var high = aggNums.length - 1
    var mid = 0
    while (low <= high) {
      mid = (high + low) / 2;
      if (aggNums(mid) <= key && (mid >= aggNums.length - 1 || aggNums(mid + 1) > key)) {
        return (mid, key - aggNums(mid))
      } else if (aggNums(mid) > key) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    return (low, key - aggNums(low))
  }
}

class GarCommitProtocol(jobId: String,
                         path: String,
                         options: Map[String, String],
                         dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) with Serializable with Logging {

  override def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    val partitionId = taskContext.getTaskAttemptID.getTaskID.getId
    if (options.contains(GeneralParams.offsetStartChunkIndexKey)) {
      // offset chunk file name, looks like chunk0
      val chunk_index = options.get(GeneralParams.offsetStartChunkIndexKey).get.toInt + partitionId
      return f"chunk$chunk_index"
    }
    if (options.contains(GeneralParams.aggNumListOfEdgeChunkKey)) {
      // edge chunk file name, looks like part0/chunk0
      val jValue = parse(options.get(GeneralParams.aggNumListOfEdgeChunkKey).get)
      implicit val formats = DefaultFormats  // initialize a default formats for json4s
      val aggNums: List[Int] = Extraction.extract[List[Int]](jValue)
      val chunkPair: (Int, Int) = GarCommitProtocol.binarySearchPair(aggNums, partitionId)
      val vertex_chunk_index: Int = chunkPair._1
      val edge_chunk_index: Int = chunkPair._2
      return f"part$vertex_chunk_index/chunk$edge_chunk_index"
    }
    // vertex chunk file name, looks like chunk0
    return f"chunk$partitionId"
  }
}
