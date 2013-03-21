package spark.examples

import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark._
import spark.SparkContext._

object TachyonJob {
  def printTimeMs(MSG: String, startTimeMs: Long, EndMsg: String) {
    val timeTakenMs = System.currentTimeMillis() - startTimeMs
    println(MSG + " APPLICATION used " + timeTakenMs + " ms " + EndMsg)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: ./run spark.examples.TachyonGrep <SchedulerMaster> "
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
    val warm = sc.parallelize(1 to 1000, 100).map(i => {
        var sum = 0
        for (i <- 0 until 1000) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    var startTimeMs = System.currentTimeMillis()

    ///////////////////////////////////////////////////////////////////////////////
    //  Clean data.
    ///////////////////////////////////////////////////////////////////////////////
    val midStartTimeMs = startTimeMs
    var InputPath: String = args(1) + "/" + jobId
    var OutputPath: String = args(2) + "/" + jobId + "/cleanedData"
    // val rawFile = sc.readFromTachyon[String](InputPath)
    val rawFile = sc.readFromByteBufferTachyon(InputPath)
    // println(rawFile.count())
    val cleanedData = rawFile.map(buf => {
      val charsBuf = buf.asCharBuffer
      val length = charsBuf.limit()
      var sum = 0
      val stringArray: ArrayList[String] = new ArrayList[String]()
      val charArray: Array[Char] = new Array[Char](10000)
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

    val keywords = Array("the", "a", "is", "in", "at", "v", "we", "1", "0", "10")
    InputPath = OutputPath
    for (round <- 0 until 10) {
      OutputPath = args(2) + "/" + jobId + "/count" + round + keywords(round)
      val data = sc.readFromByteBufferTachyon(InputPath)
      val result = data.map(buf => {
        val charsBuf = buf.asCharBuffer
        val length = charsBuf.limit()
        var sum = 0
        val charArray: Array[Char] = new Array[Char](10000)
        var currentPos: Int = 0
        for (i <- 0 until length) {
          charArray(currentPos) = charsBuf.get()
          if (charArray(currentPos) == '\n') {
            if (String.valueOf(charArray, 0, currentPos).contains(keywords(round))) {
              sum = sum + 1
            }
            currentPos = 0
          } else {
            currentPos += 1
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
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Word Count
    ///////////////////////////////////////////////////////////////////////////////
    for (round <- 0 until 10) {
      OutputPath = args(2) + "/" + jobId + "/wordcount" + round
      val data = sc.readFromByteBufferTachyon(InputPath)
      val counts = data.flatMap(buf => {
        val charsBuf = buf.asCharBuffer
        val length = charsBuf.limit()
        var sum = 0
        val stringArray: ArrayList[String] = new ArrayList[String]()
        val charArray: Array[Char] = new Array[Char](10000)
        var currentPos: Int = 0
        for (i <- 0 until length) {
          charArray(currentPos) = charsBuf.get()
          if (charArray(currentPos) == '\n') {
            val str = String.valueOf(charArray, 0, currentPos)
            if (str.contains("")) {
              stringArray.add(str)
            }
            currentPos = 0
          } else {
            currentPos += 1
          }
        }
        stringArray
        val res : Array[String] = stringArray.toArray( new Array[String](stringArray.size()) )
        res.toSeq
      }).map(word => (word, 1))
        .reduceByKey(_ + _)

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
    }

    System.exit(0)
  }
}