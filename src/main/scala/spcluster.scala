// scalastyle:off

import scala.util.Try
import scala.util.matching._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector, Vectors}
import org.apache.spark.rdd.RDD


object SpCluster {
  def main(cmdLineArgs: Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    val parsedData = acquireRDD(sc, "/tmp/scf2013.ascii")

    parsedData.saveAsTextFile("/tmp/filtered_copy_directory")

    val clusters = trainKMeans(parsedData)

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

  def acquireRDD(sc: SparkContext, fname: String): RDD[LinAlgVector] = {

    println("Loading file")
    val data = sc.textFile(fname)
    println("File loaded. Class " + data.getClass.getName)
    // println("\nNumber of elements in the Resilient Distributed Dataset: " + data.count)

    // to parse this text file is difficult, with "numeric" tokens like 14.02098.0,
    // 422.51295.0, and 0.01294.0, so the ETL has to handle exceptions because the
    // raw data is not clean
    val parsedData = data.map(
      s => Vectors.dense(s.split("  *").map(
             token => Try(token.toDouble).getOrElse(0.00)
           )
      )
    ).cache()

    println("Data parsed. Class " + parsedData.getClass.getName)
    parsedData
  }

  def trainKMeans(rdd: RDD[LinAlgVector], numClusters: Int = 5, numIterations: Int = 30):
    KMeansModel = {

      val clusters = KMeans.train(rdd, numClusters, numIterations)
      // println("Clusters found. Class " + clusters.getClass.getName)
      clusters
    }

}
