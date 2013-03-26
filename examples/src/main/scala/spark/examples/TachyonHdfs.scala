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
  var _BLOCK_SIZE_BYTES: Int = -1
  var _BLOCKS_PER_FILE: Int = -1
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

  def getSlots(sc: SparkContext) {
    // Warm JVM and Get all slots
    val WARMUP_NUM = 10
    val warm = sc.parallelize(1 to WARMUP_NUM, WARMUP_NUM).map(i => {
        var sum = 0
        for (i <- 0 until WARMUP_NUM) {
          sum += i
        }
        sum
      }).collect()
  }

  def generateLocFile(sc: SparkContext, filePrefix: String, files: Int) {
    getSlots(sc)
    // write location file.
    val pIds = sc.parallelize(0 until files, files)
    pIds.map(i => {
        var sum = 0
        for (i <- 0 until 3000000) {
          sum += i
        }

        val tachyonClient = SparkEnv.get.tachyonClient
        val fileId = tachyonClient.createFile(filePrefix + "loc/part_" + i)
        if (fileId != -1) {
          val file = tachyonClient.getFile(fileId)
          file.open(OpType.WRITE_CACHE)
          val data = ByteBuffer.allocate(4)
          data.order(ByteOrder.nativeOrder())
          data.putInt(i)
          data.flip()
          file.append(data)
          file.close()
        }

        sum
      }).collect()
  }

  def writeFiles(sc: SparkContext, filePrefix: String, files: Int, tachyon: Boolean,
    BLOCK_SIZE_BYTES: Int, BLOCKS_PER_FILE: Int) {

    System.out.println("TachyonHdfs writeFiles...");

    getSlots(sc)

    val ids = new ArrayBuffer[Int]()
    for (i <- 0 until files) {
      ids.append(i)
    }

    // write location file.
    val pIds = sc.parallelize(ids, files)
    pIds.map(i => {
        var sum = 0
        for (j <- 0 until 30) {
          for (i <- 0 until 10000000) {
            sum += i
          }
        }
        sum
      }).collect()

    waitToMs(WAKEUP_TIMEMS)

    var location : java.lang.String = filePrefix
    if (!tachyon) {
      location = filePrefix.substring(7)
      location = location.substring(location.indexOf("/"))
    }
    val tachyonFile = sc.readFromByteBufferTachyon(location + "loc")
    // System.out.println(tachyonFile.collect().toSeq)

    val starttimeMs = System.currentTimeMillis
    System.out.println(tachyonFile.map(buf => {
        val i = buf.getInt()
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        rawBuf.order(ByteOrder.nativeOrder());
        for (k <- 0 until BLOCK_SIZE_BYTES / 4) {
          rawBuf.putInt(k);
        }
        rawBuf.flip()

        val starttimeMs = System.currentTimeMillis

        if (tachyon) {
          val tachyonClient = SparkEnv.get.tachyonClient
          val fileId = tachyonClient.createFile(filePrefix + "data/part_" + i)
          if (fileId == -1) {
            throw new RuntimeException("Failed to create tachyon file " + filePrefix + "data/part_" + i)
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
          val outputStream = fs.create(new Path(filePrefix + "data/part_" + i))
          for (k <- 0 until BLOCKS_PER_FILE) {
            outputStream.write(rawBuf.array, 0, rawBuf.limit())
          }
          outputStream.close()
        }
        (System.currentTimeMillis - starttimeMs)
      }).collect().toSeq)
    val timeusedMs = System.currentTimeMillis - starttimeMs + 1
    val throughput = (1000L * BLOCK_SIZE_BYTES * BLOCKS_PER_FILE * files) / timeusedMs / 1024 / 1024
    System.out.println("TEST_CASE " + TEST_CASE + " took " + timeusedMs + " ms. Throughput is " +
      throughput + " MB/sec. From " + starttimeMs + " to " + (starttimeMs + timeusedMs))
  }

  def readFiles(sc: SparkContext, filePrefix: String, files: Int, tachyon: Boolean,
    BLOCK_SIZE_BYTES: Int, BLOCKS_PER_FILE: Int) {

    System.out.println("TachyonHdfs readFiles...");

    getSlots(sc)

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

    var location = filePrefix
    if (!tachyon) {
      location = filePrefix.substring(7)
      location = location.substring(location.indexOf("/"))
    }
    val tachyonFile = sc.readFromByteBufferTachyon(location + "loc")

    val starttimeMs = System.currentTimeMillis
    System.out.println(tachyonFile.map(buf => {
        val i = buf.getInt()
        var sum: Long = 0
        val rawBuf = ByteBuffer.allocate(BLOCK_SIZE_BYTES)
        val starttimeMs = System.currentTimeMillis
        if (tachyon) {
          val tachyonClient = SparkEnv.get.tachyonClient
          val file = tachyonClient.getFile(filePrefix + "data/part_" + i)
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
          val inputStream = fs.open(new Path(filePrefix + "data/part_" + i))
          var total = BLOCKS_PER_FILE * BLOCK_SIZE_BYTES
          while (total > 0) {
            total -= inputStream.read(rawBuf.array, 0, rawBuf.capacity())
          }
          inputStream.close()
        }
        (System.currentTimeMillis - starttimeMs)
      }).collect())
    val timeusedMs = System.currentTimeMillis - starttimeMs + 1
    val throughput = (1000L * BLOCK_SIZE_BYTES * BLOCKS_PER_FILE * files) / timeusedMs / 1024 / 1024
    System.out.println("TEST_CASE " + TEST_CASE + " took " + timeusedMs + " ms. Throughput is " +
      throughput + " MB/sec. From " + starttimeMs + " to " + (starttimeMs + timeusedMs))
  }

  def main(args: Array[String]) {
    if (args.length != 7) {
      System.out.println("./run spark.examples.TachyonHdfs <MESOS_MASTER_ADDR> " +
        "<BLOCK_SIZE_BYTES> <BLOCKS_PER_FILE> <FILES_PREFIX> <NUMBER_OF_FILES> <WAKEUP_TIME_SEC>" +
        "<TEST_CASE(1-4)>");

      System.exit(-1)
    }

    _BLOCK_SIZE_BYTES = args(1).toInt
    _BLOCKS_PER_FILE = args(2).toInt
    WAKEUP_TIMEMS = args(5).toLong * 1000
    TEST_CASE =args(6).toInt

    MSG = "BLOCK_SIZE_BYTES: " + _BLOCK_SIZE_BYTES + " BLOCKS_PER_FILE: " + _BLOCKS_PER_FILE;
    MSG += " FILES_PREFIX: " + args(3) + " NUMBER_OF_FILES: " + args(4).toInt;
    MSG += " TEST_CASE: " + TEST_CASE

    System.out.println("TachyonHdfs is starting : " + MSG);

    val sc = new SparkContext(args(0), args(3),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    if (TEST_CASE == 0) {
      generateLocFile(sc, args(3), args(4).toInt)
      writeFiles(sc, args(3), args(4).toInt, true, _BLOCK_SIZE_BYTES, _BLOCKS_PER_FILE)
    } else if (TEST_CASE == 1) {
      writeFiles(sc, args(3), args(4).toInt, true, _BLOCK_SIZE_BYTES, _BLOCKS_PER_FILE)
    } else if (TEST_CASE == 2) {
      readFiles(sc, args(3), args(4).toInt, true, _BLOCK_SIZE_BYTES, _BLOCKS_PER_FILE)
    } else if (TEST_CASE == 3) {
      writeFiles(sc, args(3), args(4).toInt, false, _BLOCK_SIZE_BYTES, _BLOCKS_PER_FILE)
    } else if (TEST_CASE == 4) {
      readFiles(sc, args(3), args(4).toInt, false, _BLOCK_SIZE_BYTES, _BLOCKS_PER_FILE)
    } else {
      throw new RuntimeException("TEST_CASE " + TEST_CASE + " is out of the range.")
    }

    println(MSG)
  }
}
