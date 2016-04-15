// scalastyle:off

import scala.util.matching._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors


object SpCluster {
  def main(cmdLineArgs: Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    println("Loading file")
    val data = sc.textFile("/tmp/scf2013.ascii")
    println("File loaded. Class " + data.getClass.getName)
    // println("\nNumber of elements in the Resilient Distributed Dataset: " + data.count)

    // to parse this text file is difficult, with "numeric" tokens like 14.02098.0,
    // 422.51295.0, and 0.01294.0
    val parsedData = data.map(
      // s if (s != "") => Vectors.dense(s.split("  *").map(_.toDouble))
      s => if (s.matches("^.*[0-9]")) {
             try {
               val v = Vectors.dense(s.trim.split("  *").map(_.toDouble))
               // println(v.toJson)
               v
             } catch {
               case ex: NumberFormatException => {
                 // println("Exception parsing line: " + s)
                 val v = Vectors.dense(Array(0.0))
                 v
               }
             }
           } else {
             // println("Non numeric line: " + s)
             val v = Vectors.dense(Array(0.0))
             v
           }
    ).cache()

    println("Data parsed. Class " + parsedData.getClass.getName)
    parsedData.saveAsTextFile("/tmp/filtered_copy_directory")

    // Cluster the data using KMeans
    val numClusters = 5
    val numIterations = 30
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    println("Clusters found. Class " + clusters.getClass.getName)

    // print the centers of the clusters returned by the training of KMeans
    for { i <- 0 until clusters.k } {
      println(clusters.clusterCenters(i).toJson)
    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    clusters.save(sc, "/tmp/mymodel.kmeans")
    val sameModel = KMeansModel.load(sc, "/tmp/mymodel.kmeans")

    sc.stop()
  }

}
