package spark.examples

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import spark._

import tachyon.client._

object TachyonHdfs {
  var BLOCK_SIZE_BYTES: Int = -1
  var BLOCKS_PER_FILE: Int = -1
  var WAKEUP_TIMEMS: Long = -1;

  def waitToMs(wakeupTimeMs: Long) {
    if (System.currentTimeMillis > wakeupTimeMs) {
      throw new RuntimeException("wakeupTimeMs " + wakeupTimeMs +
        " is older than currentTimeMillis " + System.currentTimeMillis);
    }
  }

  def writeFiles(sc: SparkContext, filePrefix: String, files: Int) {
    System.out.println("TachyonHdfs writeFiles...");

    val ids = new ArrayBuffer[Int]()
    for (i <- 0 until files) {
      ids.append(i)
    }

    val pIds = sc.parallelize(ids, files)

    // Warm JVM
    pIds.map(i => {
        var sum = 0
        for (i <- 0 until 10000000) {
          sum += i
        }
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    pIds.map(i => {
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        rawBuf.order(ByteOrder.nativeOrder());
        for (k <- 0 until BLOCK_SIZE_BYTES / 4) {
          rawBuf.putInt(k);
        }
        rawBuf.flip()

        val starttimeMs = System.currentTimeMillis
        val tachyonClient = SparkEnv.get.tachyonClient
        val fileId = tachyonClient.createFile(filePrefix + "_" + i)
        val file = tachyonClient.getFile(fileId)
        file.open(OpType.WRITE_CACHE)
        for (k <- 0 until BLOCKS_PER_FILE) {
          file.append(rawBuf);
        }
        file.close()
        (System.currentTimeMillis - starttimeMs)
      }).collect()
  }

  def readFiles(sc: SparkContext, filePrefix: String, files: Int) {
    System.out.println("TachyonHdfs readFiles...");

    val ids = new ArrayBuffer[Int]()
    for (i <- 0 until files) {
      ids.append(i)
    }

    val pIds = sc.parallelize(ids, files)

    // Warm JVM
    pIds.map(i => {
        var sum = 0
        for (i <- 0 until 10000000) {
          sum += i
        }
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    pIds.map(i => {
        var sum: Long = 0
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        val starttimeMs = System.currentTimeMillis
        val tachyonClient = SparkEnv.get.tachyonClient
        val file = tachyonClient.getFile(filePrefix + "_" + i)
        file.open(OpType.READ_TRY_CACHE)
        val inBuf = file.readByteBuffer()
        for (k <- 0 until BLOCKS_PER_FILE) {
          inBuf.get(rawBuf.array());
          sum += rawBuf.get(k % 16)
        }
        file.close()
        (System.currentTimeMillis - starttimeMs)
      }).collect()
  }

  def main(args: Array[String]) {
    if (args.length != 7) {
      System.out.println("./run spark.examples.TachyonHdfs <MESOS_MASTER_ADDR> " +
        "<BLOCK_SIZE_BYTES> <BLOCKS_PER_FILE> <FILES_PREFIX> <NUMBER_OF_FILES> <WAKEUP_TIMEMS>");

      System.exit(-1)
    }

    System.out.println("TachyonHdfs is starting...");

    val sc = new SparkContext(args(0), args(3),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    BLOCK_SIZE_BYTES = args(1).toInt
    BLOCKS_PER_FILE = args(2).toInt
    WAKEUP_TIMEMS = args(6).toLong

    writeFiles(sc, args(3), args(4).toInt);

    readFiles(sc, args(3), args(4).toInt);
  }
}
