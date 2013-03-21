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
    val JOB = "TachyonGenerateTextData " + jobId + " : " + args(1) + " to " + args(2)
    val sc = new SparkContext(args(0), JOB,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    var startTimeMs = System.currentTimeMillis()
    val midStartTimeMs = startTimeMs
    var InputPath: String = args(1) + "/" + jobId
    var OutputPath: String = args(2) + "/" + jobId + "/grepresult"
    val rawFile = sc.readFromTachyon[String](InputPath)
    val filtered = rawFile.filter(line => line.contains("95"))
    filtered.saveToTachyon(InputPath, OutputPath)
    printTimeMs(JOB, midStartTimeMs, "Grep")

    System.exit(0)
  }
}