package spark

import collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.nio.ByteBuffer

import tachyon.client._

class TachyonIntRDD(
    @transient sc: SparkContext,
    val files: java.util.List[java.lang.Integer])
  extends RDD[Int](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val tachyonClient = sc.env.tachyonClient
    val array = new Array[Partition](files.size())
    val locations = tachyonClient.getFilesHosts(files);
    for (i <- 0 until files.size()) {
      array(i) = new TachyonRDDPartition(id, files.get(i), locations.get(i).asScala)
    }
    array
  }

//   @transient
//   lazy val locations_ = {
//     val TC = sc.env.trexClient
//     val rdd = TC.getRdd(rddId)
//     val numPartitions = rdd.getNumPartitions()
//     val rddInfo = rdd.getRddInfo()
//     val hashMap = HashMap[Int, String]()

//     for (i <- 0 until rddInfo.getNumOfPartitions) {
//       val locations = rddInfo.getPartitions.get(i).getInMemoryLocations
//       if (locations != null && locations.size() > 0) {
//         hashMap.put(i, locations.get(0).getHost)
//       }
//     }

//     hashMap
// //    val blockManager = SparkEnv.get.blockManager
// //    /*val locations = blockIds.map(id => blockManager.getLocations(id))*/
// //    val locations = blockManager.getLocations(blockIds)
// //    HashMap(blockIds.zip(locations):_*)
  // }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[Int] = {
    val tachyonClient = SparkEnv.get.tachyonClient
    val fileId = theSplit.asInstanceOf[TachyonRDDPartition].index
    val file = tachyonClient.getFile(fileId)
    file.open(tachyon.client.OpType.READ_TRY_CACHE)
    var tBuf = file.readByteBuffer()
    val size = tBuf.limit() / 4
    val buf = tBuf.asIntBuffer()
    val ret: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (k <- 0 until size) {
      ret += buf.get
    }
    file.close()
    ret.iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[TachyonRDDPartition].locations
    // Array(locations_(split.asInstanceOf[TachyonRDDPartition].idx))
  }
}