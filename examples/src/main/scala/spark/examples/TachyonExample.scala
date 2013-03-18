package spark.examples

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import spark._
import SparkContext._

import tachyon.client.TachyonClient

object TachyonExample {
  val folder = "/TachyonExample"

  def endToEndTest(args: Array[String]) {
    System.out.println("TachyonExample end to end test is starting...");

    val sc = new SparkContext(args(0), "TachyonExample")
    SparkEnv.get.tachyonClient.deleteFile(folder)

    val data = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    val pData = sc.parallelize(data)
    pData.saveToTachyon(folder + "/ori")

    val res = pData.filter(x => (x % 2 == 0))
    var localValue = res.collect()
    println("++++++++++++\n" + localValue.deep.mkString("\n"))
    res.saveToTachyon(folder + "/res")

    val tData = sc.readFromTachyon[Int](folder + "/ori")
    tData.collect().foreach(ele => {System.out.print("ORIX: " + ele + " :ori\n")})
    System.out.println("********************************")
    val tRes = sc.readFromTachyon[Int](folder + "/res")
    tRes.collect().foreach(ele => {System.out.print("RESX: " + ele + " :res\n")})
  }

  def generateBinaryCodeTest(args: Array[String]) {
    System.out.println("TachyonExample generateTestBinaryCode is starting...");

    var sc = new SparkContext(args(0), "TachyonExample-GenerateTestBinaryCode")
    val data = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    val pData = sc.parallelize(data)
    val dependencyId = pData.saveToTachyon(folder + "/ori")
    val dependency = SparkEnv.get.tachyonClient.getClientDependencyInfo(dependencyId)
    val rdd = SparkEnv.get.closureSerializer.newInstance().deserialize[RDD[_]](dependency.data.get(0))
    rdd.resetSparkContext(sc)
    val arraybuffer = new ArrayBuffer[Int]()
    for (i <- 0 until 1) {
      arraybuffer.append(i)
    }
    rdd.tachyonRecompute(dependency, arraybuffer)
  }

  def main(args: Array[String]) {
    endToEndTest(args);
    generateBinaryCodeTest(args)
    System.exit(1);
  }
}