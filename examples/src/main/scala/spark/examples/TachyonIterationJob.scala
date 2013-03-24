package spark.examples

import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.HashMap
import java.util.Iterator
import java.util.Map

import spark._
import spark.SparkContext._

object TachyonIterationJob {
  def printTimeMs(MSG: String, startTimeMs: Long, EndMsg: String) {
    val timeTakenMs = System.currentTimeMillis() - startTimeMs
    println(MSG + " APPLICATION used " + timeTakenMs + " ms " + EndMsg)
  }

  def main(args: Array[String]) {
    if (args.length != 6 && args.length != 5) {
      println("Usage: ./run spark.examples.TachyonIterationJob <SchedulerMaster> "
        + "<InputData> <OutputData> <Iterations> <sleep_ms> [<load>]")
      System.exit(-1)
    }

    val iterations = args(3).toInt
    val sleep_ms = args(4).toInt
    val JOB = "TachyonIterationJob " + " : "
    val sc = new SparkContext(args(0), JOB,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    ///////////////////////////////////////////////////////////////////////////////
    //  Warm up.
    ///////////////////////////////////////////////////////////////////////////////
    val WARMUP_NUM = 10
    val warm = sc.parallelize(1 to WARMUP_NUM, WARMUP_NUM).map(i => {
        var sum = 0
        for (i <- 0 until WARMUP_NUM) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    var startTimeMs = System.currentTimeMillis()

    ///////////////////////////////////////////////////////////////////////////////
    //  Load data into memory
    ///////////////////////////////////////////////////////////////////////////////
    var midStartTimeMs = startTimeMs
    var InputPath: String = args(1)
    var rawFile = sc.readFromByteBufferTachyon(InputPath)
    if (args.length == 6) {
      val loadData = rawFile.map(buf => {
        1
      }).reduce(_ + _)

      printTimeMs(JOB, midStartTimeMs, "Load data into memory: " + InputPath + " ; " + loadData
        + " files in total")
      System.exit(0)
    }

    ///////////////////////////////////////////////////////////////////////////////
    //  Iterations
    ///////////////////////////////////////////////////////////////////////////////
    var OutputPath: String = ""
    for (i <- 0 until iterations) {
      midStartTimeMs = System.currentTimeMillis()
      OutputPath = args(2) + "/" + i
      if (i == 0) {
        InputPath = args(1)
      } else {
        InputPath = args(2) + "/" + (i - 1)
      }

      rawFile = sc.readFromByteBufferTachyon(InputPath)

      rawFile.saveToTachyon(InputPath, OutputPath, (srcBuf: ByteBuffer) => {
        val bytes = new Array[Byte](srcBuf.limit())
        srcBuf.get(bytes, 0, srcBuf.limit())
        val dstBuf = ByteBuffer.wrap(bytes)
        dstBuf.limit(srcBuf.limit())
        Thread.sleep(0)
        dstBuf
      })

      printTimeMs(JOB, midStartTimeMs, " Iteration " + i + " from " + InputPath +
        " to " + OutputPath)
    }

    printTimeMs(JOB, startTimeMs, "EndToEnd!!!!!")

    System.exit(0)
  }
}