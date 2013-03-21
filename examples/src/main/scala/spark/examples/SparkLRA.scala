package spark.examples

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._

/**
 * Logistic regression based classification with adaboost
 */
object SparkLRA {
  val N = 10000  // Number of data points
  val D = 1   // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 3
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double, var weight: Double)

  def generateData = {
    def generatePoint(i: Int ) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y, 1.0)
    }
    Array.tabulate(N)(generatePoint)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: SparkLRA <master> <path> <slices> <generate_data(true|false)>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkLRA",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val path = args(1)
    val generate_data = args(3).equals("true")

    var points: RDD[DataPoint] = null
    if (generate_data) {
      val numSlices = args(2).toInt
      points = sc.parallelize(generateData, numSlices).cache()
      println("Saving to " + path)
      points.saveToTachyon(path)
      // points.saveAsObjectFile(path)
    } else {
      println("Reading from " + path)
      points = sc.readFromTachyon[DataPoint](path)
      // points = sc.objectFile[DataPoint](path)
    }

    // Initialize w to a random value
    var w = Vector(D, i => 2 * i - 1)
    println("Initial w: " + w)

    var newPoints: RDD[DataPoint] = null
    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      points = points.map(p => {DataPoint(p.x, p.y, p.weight + 1)})
      // if (newPoints == null) {
      //   newPoints = points
      // }
      // val gradient = points.map { p =>
      //   p.weight * (1 / (1 + exp(-p.y * (w.dot(p.x))) - 1) * p.y * p.x
      // }.reduce(_ + _) / points.map(_.weight).reduce(_ + _)
      // w -= gradient
      // val newPredictions = points.map(point => if (w.dot(point.x) > 0) 1 else -1))
      // val epsilon = 0 //FIXME: Compute the error rate of the new predictions: fraction of
      // //points where points[i].y = newPredictions[i]
      // val alpha = log((1 - epsilon) / epsilon) / 2
      // val newUnnormalizedWeights = weights.map(weight => weight*exp(alpha*newPredictions[i]*points[i].y))
      // val totalWeight = newUnnormalizedWeights.sum
      // val newWeights = newUnnormalizedWeights.map(_ / totalWeight)
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
