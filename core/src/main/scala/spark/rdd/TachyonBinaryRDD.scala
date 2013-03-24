package spark

import collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.nio.ByteBuffer

import tachyon.client._

class TachyonByteBufferRDD(
    @transient sc: SparkContext,
    val files: java.util.List[java.lang.Integer])
  extends RDD[ByteBuffer](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val tachyonClient = SparkEnv.get.tachyonClient
    val array = new Array[Partition](files.size())
    val locations = tachyonClient.getFilesHosts(files)
    for (i <- 0 until files.size()) {
      array(i) = new TachyonRDDPartition(id, files.get(i), locations.get(i).asScala)
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext) = new Iterator[ByteBuffer] {
    val tachyonClient = SparkEnv.get.tachyonClient
    val fileId = theSplit.asInstanceOf[TachyonRDDPartition].index
    val file = tachyonClient.getFile(fileId)
    file.open(tachyon.client.OpType.READ_TRY_CACHE)
    val buf = file.readByteBuffer()
    var finished = false

    override def hasNext: Boolean = {
      return (!finished)
    }

    override def next: ByteBuffer = {
      if (finished) {
        throw new NoSuchElementException("End of stream")
      }
      finished = true
      buf
    }

    private def close() {
      try {
        file.close()
      } catch {
        case e: Exception => logWarning("Exception in TachyonFile.close()", e)
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (System.getProperty("spark.tachyon.recompute", "false").toBoolean) {
      val tachyonClient = SparkEnv.get.tachyonClient
      val locations = tachyonClient.getFileHosts(split.asInstanceOf[TachyonRDDPartition].index)
      println("Trying to get locations of the partition in recomputation " + split + " " + locations.asScala)
      return locations.asScala
    } else {
      println("Trying to get locations of the partition in normal computation " + split)
    }
    split.asInstanceOf[TachyonRDDPartition].locations
  }
}