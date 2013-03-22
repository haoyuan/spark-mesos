package spark.examples

import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark._

object TachyonGenerateCharsData {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: ./run spark.examples.TachyonGenerateCharsData <SchedulerMaster> "
        + "<InputData> <OutputData>")
      System.exit(-1)
    }

    var timeMs = System.currentTimeMillis()
    val JOB = "TachyonGenerateCharsData: " + args(1) + " to " + args(2)
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

    val file = sc.textFile(args(1))

    // val count1 = file.count()
    // val countTime1Ms = ((System.currentTimeMillis() - timeMs) / 1000)
    // timeMs = System.currentTimeMillis()

    // val count2 = file.count()
    // val countTime2Ms = ((System.currentTimeMillis() - timeMs) / 1000)
    // timeMs = System.currentTimeMillis()

    file.saveToTachyon(null, args(2), (str: String) => {
      val buf = ByteBuffer.allocate(str.length() * 2 + 2)
      buf.order(ByteOrder.nativeOrder())
      val charBuf = buf.asCharBuffer()
      charBuf.put(str)
      charBuf.put('\n')
      buf
    })

    // println(JOB + " APPLICATION Count1 used " + countTime1Ms + " sec. result " + count1)
    // println(JOB + " APPLICATION Count2 used " + countTime2Ms + " sec. result " + count2)
    println(JOB + " APPLICATION used " + ((System.currentTimeMillis() - timeMs) / 1000) + " sec")
    System.exit(0)
  }
}