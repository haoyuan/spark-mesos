package spark

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import spark._
import SparkContext._

import tachyon.client.TachyonClient

object TachyonRecompute {
  def main(args: Array[String]) {
    if (args.length < 3) {
      // TODO: Add program id parameter to let Tachyon know the re-computation program is running.
      System.err.println("Usage: TrexRecompute <Host> <TachyonAddress> " +
          "<DependencyId> [<RecomputeFilesIndices>]")
      System.exit(1)
    }

    val tachyonClient = TachyonClient.getClient(args(1))
    val dependency = tachyonClient.getClientDependencyInfo(args(2).toInt)
    val sparkContext = new SparkContext(args(0), "Recomputing dependency " + args(2))

    System.setProperty("spark.tachyon.recompute", "true")

    val WARMUP_NUM = 10
    val warm = sparkContext.parallelize(1 to WARMUP_NUM, WARMUP_NUM).map(i => {
        var sum = 0
        for (i <- 0 until WARMUP_NUM) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    val rdd = SparkEnv.get.closureSerializer.newInstance().deserialize[RDD[_]](dependency.data.get(0))
    rdd.resetSparkContext(sparkContext)
    val arraybuffer = new ArrayBuffer[Int]()
    for (i <- 3 until args.length) {
      arraybuffer.append(args(i).toInt)
    }
    rdd.tachyonRecompute(dependency, arraybuffer)

    System.exit(1)
  }
}