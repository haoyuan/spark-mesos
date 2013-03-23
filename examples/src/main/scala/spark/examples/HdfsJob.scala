package spark.examples

import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark._
import spark.SparkContext._

object HdfsJob {
  def printTimeMs(MSG: String, startTimeMs: Long, EndMsg: String) {
    val timeTakenMs = System.currentTimeMillis() - startTimeMs
    println(MSG + " APPLICATION used " + timeTakenMs + " ms " + EndMsg)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: ./run spark.examples.HdfsJob <SchedulerMaster> "
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
    var OutputPath: String = args(2) + "/" + jobId + "/cleanedData"
    // val rawFile = sc.readFromTachyon[String](InputPath)
    val rawFile = sc.textFile(InputPath)
    // println(rawFile.count())
    val cleanedData = rawFile.filter(line => line.contains("the"))
    cleanedData.saveAsTextFile(OutputPath)

    printTimeMs(JOB, midStartTimeMs, "Data Clean from " + InputPath + " to " + OutputPath)

    ///////////////////////////////////////////////////////////////////////////////
    // Count how many lines have the word
    ///////////////////////////////////////////////////////////////////////////////

    val keywords = Array('v', 'b', 'c', 'd', 'v', 'f', 'g', 'h', 'i', 'j')
    InputPath = OutputPath
    for (round <- 0 until 5) {
      midStartTimeMs = System.currentTimeMillis()
      OutputPath = args(2) + "/" + jobId + "/count" + round + keywords(round)
      val data = sc.textFile(InputPath)
      val result = data.map(str => {
        var ret = 0
        if (str.contains(keywords(round))) {
          ret = 1
        }
        ret
      })

      val count = result.reduce(_ + _)
      println(count)
      result.saveAsTextFile(OutputPath)
      printTimeMs(JOB, midStartTimeMs, "Count from " + InputPath + " to " + OutputPath)
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Word Count
    ///////////////////////////////////////////////////////////////////////////////
    for (round <- 0 until 5) {
      midStartTimeMs = System.currentTimeMillis()
      OutputPath = args(2) + "/" + jobId + "/wordcount" + round
      val data = sc.textFile(InputPath)
      val counts = data.flatMap(line => line.split(" "))
                       .map(word => (word, 1))
                       .reduceByKey(_ + _, 3)

      counts.saveAsTextFile(OutputPath)
      printTimeMs(JOB, midStartTimeMs, "WordCount" + round + " from " +
        InputPath + " to " + OutputPath)
    }
    printTimeMs(JOB, startTimeMs, "EndToEnd!!!!!")

    System.exit(0)
  }
}