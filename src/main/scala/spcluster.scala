
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._


object SpCluster {
  def main(cmdLineArgs: Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    println("Loading file")
    val data = sc.textFile("/tmp/scf2013.ascii")
    println("File loaded. Class " + data.getClass.getName +
            "\nNumber of elements in the Resilient Distributed Dataset: " + data.count)
    sc.stop()
  }

}
