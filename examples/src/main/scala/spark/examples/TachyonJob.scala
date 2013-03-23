package spark.examples

import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.HashMap
import java.util.Iterator
import java.util.Map

import spark._
import spark.SparkContext._

object TachyonJob {
  def printTimeMs(MSG: String, startTimeMs: Long, EndMsg: String) {
    val timeTakenMs = System.currentTimeMillis() - startTimeMs
    println(MSG + " APPLICATION used " + timeTakenMs + " ms " + EndMsg)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: ./run spark.examples.TachyonJob <SchedulerMaster> "
        + "<InputData> <OutputData> <JobId>")
      System.exit(-1)
    }

    val jobId = args(3).toInt
    val JOB = "TachyonJob " + jobId + " : "
    val sc = new SparkContext(args(0), JOB,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    ///////////////////////////////////////////////////////////////////////////////
    //  Warm up.
    ///////////////////////////////////////////////////////////////////////////////
    val WARMUP_NUM = 3000
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
    //  Clean data.
    ///////////////////////////////////////////////////////////////////////////////
    var midStartTimeMs = startTimeMs
    var InputPath: String = args(1) + "/" + jobId
    var rawFile = sc.readFromByteBufferTachyon(InputPath)
    val loadData = rawFile.map(buf => {
      1
    }).reduce(_ + _)

    printTimeMs(JOB, midStartTimeMs, "Load data into memory: " + InputPath + " ; " + loadData
      + " files in total")

    ///////////////////////////////////////////////////////////////////////////////
    //  Clean data.
    ///////////////////////////////////////////////////////////////////////////////
    midStartTimeMs = startTimeMs
    InputPath = args(1) + "/" + jobId
    var OutputPath: String = args(2) + "/" + jobId + "/cleanedData"
    // val rawFile = sc.readFromTachyon[String](InputPath)
    rawFile = sc.readFromByteBufferTachyon(InputPath)
    // val cleanedData = rawFile.map(buf => {buf})
    // cleanedData.saveToTachyon(InputPath, OutputPath, (oriBuf: ByteBuffer) => {
    //   val buf = ByteBuffer.allocate(oriBuf.limit())
    //   buf.order(ByteOrder.nativeOrder())
    //   val charBuf = buf.asCharBuffer()

    //   var oriCharBuf = oriBuf.asCharBuffer
    //   val length = oriCharBuf.limit()
    //   var lastChar: Char = ' '
    //   var curChar: Char = ' '
    //   for (i <- 0 until length) {
    //     curChar = oriCharBuf.get()
    //     if (curChar == ' ' && curChar == lastChar) {
    //     } else {
    //       charBuf.put(curChar)
    //       lastChar = curChar
    //     }
    //   }
    //   buf.limit(charBuf.position * 2)
    //   buf
    // })

    val cleanedData = rawFile.map(buf => {
      val charsBuf = buf.asCharBuffer
      val length = charsBuf.limit()
      var sum = 0
      val stringArray: ArrayList[String] = new ArrayList[String]()
      val charArray: Array[Char] = new Array[Char](1000)
      var currentPos: Int = 0
      for (i <- 0 until length) {
        charArray(currentPos) = charsBuf.get()
        if (charArray(currentPos) == '\n') {
          val str = String.valueOf(charArray, 0, currentPos)
          if (str.contains("the")) {
            stringArray.add(str)
          }
          currentPos = 0
        } else {
          currentPos += 1
        }
      }
      stringArray
    })

    cleanedData.saveToTachyon(InputPath, OutputPath, (str: ArrayList[String]) => {
      var sum = 0;
      for (k <- 0 until str.size) {
        sum = sum + str.get(k).length * 2 + 2
      }
      val buf = ByteBuffer.allocate(sum)
      buf.order(ByteOrder.nativeOrder())
      val charBuf = buf.asCharBuffer()

      for (k <- 0 until str.size) {
        charBuf.put(str.get(k))
        charBuf.put('\n')
      }
      buf
    })

    printTimeMs(JOB, midStartTimeMs, "Data Clean from " + InputPath + " to " + OutputPath)

    ///////////////////////////////////////////////////////////////////////////////
    // Count how many lines have the word
    ///////////////////////////////////////////////////////////////////////////////

    val keywords = Array('a', 'b', 'c', 'd', 'v', 'f', 'g', 'h', 'i', 'j')
    InputPath = OutputPath
    for (round <- 0 until 5) {
      midStartTimeMs = System.currentTimeMillis()
      OutputPath = args(2) + "/" + jobId + "/count" + round + keywords(round)
      val data = sc.readFromByteBufferTachyon(InputPath)
      val result = data.map(buf => {
        val charsBuf = buf.asCharBuffer
        val length = charsBuf.limit()
        var sum = 0
        var currentPos: Int = 0
        var good: Boolean = false
        for (i <- 0 until length) {
          var t = charsBuf.get()
          if (t == '\n') {
            if (good) {
              sum = sum + 1
            }
            good = false
            currentPos = 0
          } else {
            currentPos += 1
            if (t == keywords(round)) {
              good = true
            }
          }
        }
        sum
      })

      val count = result.reduce(_ + _)
      println(count)
      result.saveToTachyon(InputPath, OutputPath, (sum: Int) => {
        val buf = ByteBuffer.allocate(4)
        buf.order(ByteOrder.nativeOrder())
        buf.putInt(sum)
        buf.flip()

        buf
      })
      printTimeMs(JOB, midStartTimeMs, "Count from " + InputPath + " to " + OutputPath)
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Word Count
    ///////////////////////////////////////////////////////////////////////////////
    for (round <- 0 until 5) {
      midStartTimeMs = System.currentTimeMillis()
      OutputPath = args(2) + "/" + jobId + "/wordcount" + round
      val data = sc.readFromByteBufferTachyon(InputPath)
      val counts = data.flatMap(buf => {
        val charsBuf = buf.asCharBuffer
        val length = charsBuf.limit()
        var sum = 0
        val stringArray = new HashMap[String, Int]()
        val charArray: Array[Char] = new Array[Char](5000)
        var currentPos: Int = 0
        for (i <- 0 until length) {
          charArray(currentPos) = charsBuf.get()
          if (charArray(currentPos) == '\n' || charArray(currentPos) == ' ') {
            val tStr = String.valueOf(charArray, 0, currentPos)
            if (stringArray.containsKey(tStr)) {
              stringArray.put(tStr, stringArray.get(tStr) + 1)
            } else {
              stringArray.put(tStr, 1)
            }
            currentPos = 0
          } else {
            currentPos += 1
          }
        }
        val it = stringArray.entrySet().iterator()
        val result = new ArrayList[(String, Int)]
        while (it.hasNext()) {
          var entry: Map.Entry[String, Int] = it.next
          result.add((entry.getKey(), entry.getValue()))
        }
        val res : Array[(String, Int)] = result.toArray(new Array[(String, Int)](result.size()) )
        res.toSeq
      }).reduceByKey(_ + _, 5)

      counts.saveToTachyon(InputPath, OutputPath, (pairData: (String, Int)) => {
        var sum = 0;
        sum = sum + pairData._1.length * 2 + 4 + 2
        val buf = ByteBuffer.allocate(sum)
        buf.order(ByteOrder.nativeOrder())

        for (i <- 0 until pairData._1.length) {
          buf.putChar(pairData._1.charAt(i))
        }
        buf.putChar('\n')
        buf.putInt(pairData._2)
        buf.flip()

        buf
      })
      printTimeMs(JOB, midStartTimeMs, "WordCount" + round + " from " +
        InputPath + " to " + OutputPath)
    }
    printTimeMs(JOB, startTimeMs, "EndToEnd!!!!!")

    System.exit(0)
  }
}