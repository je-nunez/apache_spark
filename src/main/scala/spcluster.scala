// scalastyle:off

package mainapp


import java.io.{InputStream, File}
import java.net.URL

import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector}
import org.apache.spark.rdd.RDD


import excel2rdd.Excel2RDD
import excel2rdd.ExcelHeaderExtract
import excel2rdd.{ExcelColumnFilter, ExcelDropColumns}
// import excel2rdd.ExcelRowIdentity

import customNYFedBankSCE.NYFedBankSCE


object SpCluster {

  val saveRDDAsTxtToDir = "/tmp/filtered.rdd.copy.dir"

  val saveKMeansModelToDir = "/tmp/save.model.kmeans.dir"

  val saveKMeansModelToPMML = "/tmp/FRBNY-SCE-Housing.pmml"

  def main(cmdLineArgs: Array[String]) : Unit = {

    prepareTempDirs()

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    val excelXlsx = new Excel2RDD(openInputDataSource)

    val excelDropColumns = new ExcelDropColumns(Array(0))
    val excelTransformRow = new NYFedBankSCE()
    excelXlsx.open()
    val parsedData =
      excelXlsx.convertExcelSpreadsh2RDD("Data", ExcelHeaderExtract, excelDropColumns,
                                         excelTransformRow, sc)
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

    exploreClustersKMeans(parsedData,
      (rdd: RDD[LinAlgVector], model1: KMeansModel, model2: KMeansModel) => {
         if (model1 == null) {
           Double.MaxValue     // choose the second model
         } else if (model2 == null) {
           Double.MinValue     // choose the first
         } else {
           val wssse1 = model1.computeCost(rdd)
           val wssse2 = model2.computeCost(rdd)
           ( wssse1 / model1.k) - ( wssse2 / model2.k )
           // wssse1 - wssse2
         }
      }
    ) match {
      case Some(bestClusterModel) => {
        reportKMeanClusters(bestClusterModel, parsedData, excelXlsx)
        // Save and load model (export also in PMML/XML format. You will need
        // org.jpmml.model.JAXBUtil.unmarshalPMML(pmml) and
        // org.jpmml.evaluator.ModelEvaluatorFactory(), etc, to re-read it. R also has a package
        // for it.
        bestClusterModel.toPMML(saveKMeansModelToPMML)
        bestClusterModel.save(sc, saveKMeansModelToDir)
        val sameModel = KMeansModel.load(sc, saveKMeansModelToDir)
      }
      case None => {
        println("No K-Means clustering model could be found with the set-cardinality requested.")
      }
    }

    sc.stop()
  }

  def prepareTempDirs(): Unit = {
    FileUtils.deleteQuietly(new File(saveRDDAsTxtToDir))
    FileUtils.deleteQuietly(new File(saveKMeansModelToDir))
  }

  def openInputDataSource(): InputStream = {

    // It takes the input Excel spreadsheet on which to run the Apache Spark's K-means clustering
    // from the Center for Microeconomic Data at the Federal Reserve Bank of New York, at:
    //      https://www.newyorkfed.org/microeconomics/data.html
    // This input Excel XLSX has several spreadsheets inside, of which we use only the "Data" one
    // TODO: use caching to download this input Excel XLSX only once, although it is only 747 KB.

    val microEconomics =
      "https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx"
    val url = new URL(microEconomics)
    val conn = url.openConnection()

    conn.getInputStream()

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
   * Explore which model of K-Means clusters, for a model of ordinality "N", best fits into the
   * input RDD, among several possible cardinalities N. The model which best fits is that one
   * which has the least Within Set Sum of Squared Errors. (Of course, this leaves the question
   * open whether such a clustering set is nevertheless stable, ie., for infinitesimal variations
   * on the input data, another model is better.)
   */

  def exploreClustersKMeans(rdd: RDD[LinAlgVector],
                      chooseBestModel: (RDD[LinAlgVector], KMeansModel, KMeansModel) => Double,
                      minNumClusters: Int = 2, maxNumClusters: Int = 100, numIterations: Int = 80
      ): Option[KMeansModel] = {

    var bestClusterModel: KMeansModel = null

    for { numClusters <- minNumClusters to maxNumClusters } {
      val currClusterModel = trainKMeans(rdd, numClusters, numIterations)

      val better = chooseBestModel(rdd, bestClusterModel, currClusterModel)
           // a speed-up is to save the previous calculation for "bestClusterModel", if it doesn't
           // need to be recalculated in the next iteration
      if (better > 0.001) {
        bestClusterModel = currClusterModel
      }
    }

    Option(bestClusterModel)
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
    println("Cardinality of the K-Means clustering found: " +  kMeans.k)
  }

}
