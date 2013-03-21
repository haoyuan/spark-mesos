package spark.examples

import spark._

object ByteBufferTachyonTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "ByteBufferTachyonTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val file = sc.readFromByteBufferTachyon(args(1))
    println(file.map(buf => 1).reduce(_ + _))
    // val mapped = file.map(s => s.length).cache()
    // for (iter <- 1 to 10) {
    //   val start = System.currentTimeMillis()
    //   for (x <- mapped) { x + 2 }
    //   //  println("Processing: " + x)
    //   val end = System.currentTimeMillis()
    //   println("Iteration " + iter + " took " + (end-start) + " ms")
    // }
    System.exit(0)
  }
}
