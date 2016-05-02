// scalastyle:off

package mainapp


import java.io.File
import org.apache.commons.io.FileUtils


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector}
import org.apache.spark.rdd.RDD


import excel2rdd.Excel2RDD
import excel2rdd.ExcelHeaderExtract
import excel2rdd.{ExcelColumnFilter, ExcelColumnIdentity}


object SpCluster {

  val saveRDDAsTxtToDir = "/tmp/filtered.rdd.copy.dir"

  val saveKMeansModelToDir = "/tmp/save.model.kmeans.dir"

  def main(cmdLineArgs: Array[String]) : Unit = {

    prepareTempDirs()

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    val excelXlsx = new Excel2RDD("/tmp/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx")

    excelXlsx.open()
    val parsedData =
      excelXlsx.convertExcelSpreadsh2RDD("Data", ExcelHeaderExtract, ExcelColumnIdentity, sc)
    excelXlsx.close()

    parsedData.saveAsTextFile(saveRDDAsTxtToDir)

    if (!validateRDDForKMeans(parsedData)) {
      // This should not happen since the Excel2RDD converter tries to pad shorter rows in the
      // Excel spreadsheet to be vectors with the same dimension in the new RDD.
      System.err.println("There are vectors inside the RDD which have different dimensions.\n" +
                         "All vectors must have the same dimensions.\nAborting.")
      sc.stop()
      System.exit(1)
    }

    val clusters = trainKMeans(parsedData, numClusters = 10)

    reportKMeanClusters(clusters, parsedData, excelXlsx)

    // Save and load model
    clusters.save(sc, saveKMeansModelToDir)
    val sameModel = KMeansModel.load(sc, saveKMeansModelToDir)

    sc.stop()
  }

  def prepareTempDirs(): Unit = {
    FileUtils.deleteQuietly(new File(saveRDDAsTxtToDir))
    FileUtils.deleteQuietly(new File(saveKMeansModelToDir))
  }

  def minMaxVectorInRdd(rdd: RDD[LinAlgVector]): (Int, Int) = {
    var min = Int.MaxValue
    var max = Int.MinValue

    rdd.collect.foreach(v => {
        val v_sz = v.size
        if (v_sz < min) {
          min = v_sz
        } else if (v_sz > max) {
          max = v_sz
        }
      }
    )

    (min, max)
  }

  def validateRDDForKMeans(rdd: RDD[LinAlgVector]): Boolean = {

    // Avoid an exception because the vectors inside the RDD have different size
    // (as of the current version of Spark 1.6.1 as of April 18, 2016). This issue
    // appears because the ETL on the raw text file given, happens to generate vectors
    // with different dimensions (this is an issue with the input raw text file given,
    // as mentioned above in acquireRDD(...)).
    //
    // Exception ... org.apache.spark.SparkException: ...: java.lang.IllegalArgumentException: requirement failed
    //    at scala.Predef$.require(Predef.scala:221)
    //    at org.apache.spark.mllib.util.MLUtils$.fastSquaredDistance(MLUtils.scala:330)
    //    at org.apache.spark.mllib.clustering.KMeans$.fastSquaredDistance(KMeans.scala:595)
    //    at org.apache.spark.mllib.clustering.KMeans$$anonfun$findClosest$1.apply(KMeans.scala:569)
    //    at org.apache.spark.mllib.clustering.KMeans$$anonfun$findClosest$1.apply(KMeans.scala:563)
    //
    // The lines at MLUtils.scala:330 in the stack trace of the exception are:
    //   329       val n = v1.size
    //   330       require(v2.size == n)
    // so vectors need to be the same size (in this case, it happens that one of the vectors is
    // the calculated center of a KMeans cluster).

    // get the minimum an maximum sizes for all vectors inside the RDD
    val (min_size, max_size) = minMaxVectorInRdd(rdd)

    (min_size == max_size)
  }

  def trainKMeans(rdd: RDD[LinAlgVector], numClusters: Int = 5, numIterations: Int = 30):
      KMeansModel = {

    val clusters = KMeans.train(rdd, numClusters, numIterations)
    // println("Clusters found. Class " + clusters.getClass.getName)
    clusters
  }

  /**
   * Print the centers of the clusters returned by the training of KMeans, and the
   * Within Set Sum of Squared Errors from the samples
   */

  def reportKMeanClusters(kMeans: KMeansModel, rdd: RDD[LinAlgVector],
      originalData: Excel2RDD): Unit = {

    for { i <- 0 until kMeans.k } {
      val currKMeanCenter = kMeans.clusterCenters(i)
      println(s"Reporting center of KMeans cluster $i")
      var nonZeroColumns = 0
      // println(currKMeanCenter.toJson)

      for { j <- 0 until currKMeanCenter.size } {
        val value = currKMeanCenter(j)
        if ( value != 0.00 ) {
          val colName = originalData.getHeader(j)
          println(f"Column $j%4d $colName%-15s value: $value%.8f")
          nonZeroColumns += 1
        }
      }
      println(s"Number for non-zero columns for center of KMeans cluster $i: $nonZeroColumns")

    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = kMeans.computeCost(rdd)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }

}
