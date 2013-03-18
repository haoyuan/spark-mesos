package spark.examples

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import spark._

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

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

  def writeFiles(sc: SparkContext, filePrefix: String, files: Int, tachyon: Boolean) {
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

        if (tachyon) {
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
        } else {
          val conf = new Configuration()
          conf.set("fs.default.name", filePrefix)
          val fs = FileSystem.get(conf)
          val outputStream = fs.create(new Path(filePrefix + "_" + i))
          for (k <- 0 until BLOCKS_PER_FILE) {
            outputStream.write(rawBuf.array, 0, rawBuf.limit())
          }
          outputStream.close()
        }
        (System.currentTimeMillis - starttimeMs)
      }).collect().toSeq)
    val timeusedMs = System.currentTimeMillis - starttimeMs
    val throughput = (1000L * BLOCK_SIZE_BYTES * BLOCKS_PER_FILE * files) / timeusedMs / 1024 / 1024
    System.out.println("TEST_CASE " + TEST_CASE + " took " + timeusedMs + " ms. Throughput is " +
      throughput + " MB/sec. From " + starttimeMs + " to " + (starttimeMs + timeusedMs))
  }

  def readFiles(sc: SparkContext, filePrefix: String, files: Int, tachyon: Boolean) {
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
        val file = SparkEnv.get.tachyonClient.getFile(1)
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    val starttimeMs = System.currentTimeMillis
    System.out.println(pIds.map(i => {
        var sum: Long = 0
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        val starttimeMs = System.currentTimeMillis
        if (tachyon) {
          val tachyonClient = SparkEnv.get.tachyonClient
          val file = tachyonClient.getFile(filePrefix + "_" + i)
          file.open(OpType.READ_TRY_CACHE)
          val inBuf = file.readByteBuffer()
          for (k <- 0 until BLOCKS_PER_FILE) {
            inBuf.get(rawBuf.array());
            sum += rawBuf.get(k % 16)
          }
          file.close()
        } else {
          val conf = new Configuration()
          conf.set("fs.default.name", filePrefix)
          val fs = FileSystem.get(conf)
          val inputStream = fs.open(new Path(filePrefix + "_" + i))
          var total = BLOCKS_PER_FILE * BLOCK_SIZE_BYTES
          while (total > 0) {
            total -= inputStream.read(rawBuf.array, 0, rawBuf.capacity())
          }
          inputStream.close()
        }
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
      writeFiles(sc, args(3), args(4).toInt, true)
    } else if (TEST_CASE == 2) {
      readFiles(sc, args(3), args(4).toInt, true)
    } else if (TEST_CASE == 3) {
      writeFiles(sc, args(3), args(4).toInt, false)
    } else if (TEST_CASE == 4) {
      readFiles(sc, args(3), args(4).toInt, false)
    } else {
      throw new RuntimeException("TEST_CASE " + TEST_CASE + " is out of the range.")
    }
    MSG = "BLOCK_SIZE_BYTES: " + BLOCK_SIZE_BYTES + " BLOCKS_PER_FILE: " + BLOCKS_PER_FILE;
    MSG += " FILES_PREFIX: " + args(3) + " NUMBER_OF_FILES: " + args(4).toInt;
    MSG += " TEST_CASE: " + TEST_CASE
    println(MSG)
  }
}
