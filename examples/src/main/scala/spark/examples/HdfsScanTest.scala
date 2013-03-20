package spark.examples

import spark._

object HdfsScanTest {
  def main(args: Array[String]) {
    val timeMs = System.currentTimeMillis()
    val sc = new SparkContext(args(0), args(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val file = sc.textFile(args(1))
    println(args(2) + " TACHYON TEST Result " + file.count() + " using " +
     ((System.currentTimeMillis() - timeMs) / 1000) + " sec")
    System.exit(0)
  }
}