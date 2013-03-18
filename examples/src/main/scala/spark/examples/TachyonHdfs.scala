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
  var WAKEUP_TIMEMS: Long = -1
  var TEST_CASE: Int = -1
  var MSG: String = ""

  def waitToMs(wakeupTimeMs: Long) {
    if (System.currentTimeMillis > wakeupTimeMs) {
      throw new RuntimeException("wakeupTimeMs " + wakeupTimeMs +
        " is older than currentTimeMillis " + System.currentTimeMillis);
    }
    Thread.sleep(wakeupTimeMs - System.currentTimeMillis)
  }

  def writeFilesToTachyon(sc: SparkContext, filePrefix: String, files: Int) {
    System.out.println("TachyonHdfs writeFilesToTachyon...");

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
        val file = SparkEnv.get.tachyonClient.getFile(1)
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    val starttimeMs = System.currentTimeMillis
    System.out.println(pIds.map(i => {
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        rawBuf.order(ByteOrder.nativeOrder());
        for (k <- 0 until BLOCK_SIZE_BYTES / 4) {
          rawBuf.putInt(k);
        }
        rawBuf.flip()

        val starttimeMs = System.currentTimeMillis
        val tachyonClient = SparkEnv.get.tachyonClient
        val fileId = tachyonClient.createFile(filePrefix + "_" + i)
        if (fileId == -1) {
          throw new RuntimeException("Failed to create tachyon file " + filePrefix + "_" + i)
        }
        val file = tachyonClient.getFile(fileId)
        file.open(OpType.WRITE_CACHE)
        for (k <- 0 until BLOCKS_PER_FILE) {
          file.append(rawBuf);
        }
        file.close()
        (System.currentTimeMillis - starttimeMs)
      }).collect().toSeq)
    val timeusedMs = System.currentTimeMillis - starttimeMs
    val throughput = (1000L * BLOCK_SIZE_BYTES * BLOCKS_PER_FILE * files) / timeusedMs / 1024 / 1024
    System.out.println("TEST_CASE " + TEST_CASE + " took " + timeusedMs + " ms. Throughput is " +
      throughput + " MB/sec. From " + starttimeMs + " to " + (starttimeMs + timeusedMs))
  }

  def readFilesFromTachyon(sc: SparkContext, filePrefix: String, files: Int) {
    System.out.println("TachyonHdfs readFilesFromTachyon...");

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
        val file = SparkEnv.get.tachyonClient.getFile(1)
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    val starttimeMs = System.currentTimeMillis
    System.out.println(pIds.map(i => {
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
      }).collect())
    val timeusedMs = System.currentTimeMillis - starttimeMs
    val throughput = (1000L * BLOCK_SIZE_BYTES * BLOCKS_PER_FILE * files) / timeusedMs / 1024 / 1024
    System.out.println("TEST_CASE " + TEST_CASE + " took " + timeusedMs + " ms. Throughput is " +
      throughput + " MB/sec. From " + starttimeMs + " to " + (starttimeMs + timeusedMs))
  }

  def main(args: Array[String]) {
    if (args.length != 7) {
      System.out.println("./run spark.examples.TachyonHdfs <MESOS_MASTER_ADDR> " +
        "<BLOCK_SIZE_BYTES> <BLOCKS_PER_FILE> <FILES_PREFIX> <NUMBER_OF_FILES> <WAKEUP_TIME_SEC>" +
        "<TEST_CASE(1-2)>");

      System.exit(-1)
    }

    System.out.println("TachyonHdfs is starting...");

    val sc = new SparkContext(args(0), args(3),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    BLOCK_SIZE_BYTES = args(1).toInt
    BLOCKS_PER_FILE = args(2).toInt
    WAKEUP_TIMEMS = args(5).toLong * 1000
    TEST_CASE =args(6).toInt

    if (TEST_CASE == 1) {
      writeFilesToTachyon(sc, args(3), args(4).toInt)
    } else if (TEST_CASE == 2) {
      readFilesFromTachyon(sc, args(3), args(4).toInt)
    } else {
      throw new RuntimeException("TEST_CASE " + TEST_CASE + " is out of the range.")
    }
    MSG = "BLOCK_SIZE_BYTES: " + BLOCK_SIZE_BYTES + " BLOCKS_PER_FILE: " + BLOCKS_PER_FILE;
    MSG += " FILES_PREFIX: " + args(3) + " NUMBER_OF_FILES: " + args(4).toInt;
    MSG += " TEST_CASE: " + TEST_CASE
    println(MSG)
  }
}
