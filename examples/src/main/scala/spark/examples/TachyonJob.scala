package spark.examples

import spark._

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
    val JOB = "TachyonJob " + jobId + " : " + args(1) + " to " + args(2)
    val sc = new SparkContext(args(0), JOB,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val warm = sc.parallelize(1 to 1000, 1000).map(i => {
        var sum = 0
        for (i <- 0 until 1000) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    var startTimeMs = System.currentTimeMillis()
    val midStartTimeMs = startTimeMs
    var InputPath: String = args(1) + "/" + jobId
    var OutputPath: String = args(2) + "/" + jobId + "/grepresult"
    // val rawFile = sc.readFromTachyon[String](InputPath)
    val rawFile = sc.readFromByteBufferTachyon(InputPath)
    // println(rawFile.count())
    rawFile.map(buf => {
      val charsBuf = buf.asCharBuffer
      val length = charsBuf.limit()
      var sum = 0
      for (i <- 0 until length) {
        if (charsBuf.get() == 'v') {
          sum += 1
        }
      }
      sum
    }).collect().foreach(println)
    // val filtered = rawFile.filter(line => line.contains("the"))
    // filtered.saveToTachyon(InputPath, OutputPath)
    printTimeMs(JOB, midStartTimeMs, "Grep")

    System.exit(0)
  }
}