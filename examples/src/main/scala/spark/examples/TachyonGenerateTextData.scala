package spark.examples

import spark._

object TachyonGenerateTextData {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: ./run spark.examples.TachyonGenerateTextData <SchedulerMaster> "
        + "<InputData> <OutputData>")
      System.exit(-1)
    }

    val timeMs = System.currentTimeMillis()
    val JOB = "TachyonGenerateTextData: " + args(1) + " to " + args(2)
    val sc = new SparkContext(args(0), JOB,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val file = sc.textFile(args(1))
    file.saveToTachyon(null, args(2))
    println(JOB + " APPLICATION used " + ((System.currentTimeMillis() - timeMs) / 1000) + " sec")
    System.exit(0)
  }
}