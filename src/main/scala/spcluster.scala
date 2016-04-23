// scalastyle:off

import scala.annotation.switch
import scala.collection.immutable.StringOps
import scala.util.Try
import scala.util.matching._

import java.io.{File, FileInputStream, FileWriter, BufferedWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector, Vectors}
import org.apache.spark.rdd.RDD

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFCell
import org.apache.poi.ss.usermodel.Cell


object SpCluster {
  def main(cmdLineArgs: Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    /*
    convertExcelSpreadsh2RDD(sc, "/tmp/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx",
                             "Data")
     */

    val parsedData = acquireRDD(sc, "/tmp/scf2013.ascii")

    parsedData.saveAsTextFile("/tmp/filtered_copy_directory")

    if (!validateRDDForKMeans(parsedData)) {
      System.err.println("There are vectors inside the RDD which have different dimensions.\n" +
                         "All vectors must have the same dimensions.\nAborting.")
      sc.stop()
      System.exit(1)
    }

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

  def minMaxVectorInRdd(rdd: RDD[LinAlgVector]): (Int, Int) = {
    var min = Int.MaxValue
    var max = Int.MinValue

    rdd.collect().foreach(v => {
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


  def iterExcelRows(xlsxFName: String, sheetName: String, rowFunction: XSSFRow => Unit): Unit = {
    val excelFileToRead = new FileInputStream(xlsxFName)
    val xlsWbk = new XSSFWorkbook(excelFileToRead)

    val xlsSheet = xlsWbk.getSheet(sheetName)

    val rows = xlsSheet.rowIterator()    // get an iterator over the rows

    while (rows.hasNext()) {
      rowFunction(rows.next().asInstanceOf[XSSFRow])
    }
    xlsWbk.close()

  }

  def findMaxColumnInExcelSpreadsh(xlsxFName: String, sheetName: String): Int = {

    var maxCol: Int = -1

    iterExcelRows(xlsxFName, sheetName,
                 (row: XSSFRow) => {
                   val maxColRow = row.getLastCellNum - 1
                   if (maxColRow > maxCol) { maxCol = maxColRow }
                 }
    )
    maxCol
  }

  def convertDouble(d: Double): String = {

    val directConversion = "%f".format(d)

    if (directConversion.indexOf('.') >= 1) {
      directConversion.reverse.dropWhile(_ == '0').dropWhile(_ == '.').reverse
      // dropWhile seems slightly faster than tail-recursion to remove non-significant '0'
      // generated when reading the floating-points in the source Excel cells
    } else {
      directConversion
    }
  }

  // A very simple converter of an Excel XLSX spreadsheet to a CSV text file. In particular, it
  // fills empty cells with '0' (ie., NA in the source are '0' for CSV -see below). Empty rows
  // in the spreadsheet are omitted (nothing is outputted in the CSV for them), for we want to
  // convert at the end to a Spark RDD and completely empty rows in a RDD don't have too much
  // meaning -except to increase the number of rows in the RDD and those measures that depend on
  // the number of rows, like mean, standard deviation, etc, where the number of rows appears in
  // the denominator.
  // (A subsequent version will omit the intermediate CSV of the Excel spreadsheet)
  // There are other, more complete Excel XLSX - to -> CSV converters as examples of Apache POI:
  //    https://poi.apache.org/spreadsheet/examples.html

  def excelSheetToCsv(xlsxFName: String, sheetName: String, maxColumnIdx: Int, csvFName: String):
      Unit = {

    val fillValue = "0"        // string for filling empty cells (ie., to fill NA)
    val csvSeparator = ","
    val cvsOut = new BufferedWriter(new FileWriter(csvFName))
    val cvsLine = new StringBuilder(8 * 1024)

    iterExcelRows(xlsxFName, sheetName,
      (row: XSSFRow) => {
        cvsLine.clear()
        val cells = row.cellIterator    // get an iterator over the cells in this row
        var previousCellCol: Int = -1

        while (cells.hasNext)
        {
          val cell = cells.next.asInstanceOf[XSSFCell]
          val currentCol = cell.getColumnIndex

          def fillEmptyCells(): String = {
            val strPreffix = if (previousCellCol > -1) csvSeparator else ""
            val numColsJumped = currentCol - (previousCellCol + 1)

            strPreffix + (( fillValue + csvSeparator ) * numColsJumped)
          }

          cvsLine.append(fillEmptyCells)
          previousCellCol = currentCol

          (cell.getCellType: @switch) match {
            case Cell.CELL_TYPE_STRING => cvsLine.append(cell.getStringCellValue)
            case Cell.CELL_TYPE_NUMERIC => cvsLine.append(convertDouble(cell.getNumericCellValue))
            case Cell.CELL_TYPE_BOOLEAN => cvsLine.append(cell.getBooleanCellValue.toString)
            case _ => cvsLine.append("Unknown value at Row: " + (row.getRowNum + 1) +
                                     " Column: " + (currentCol + 1))     // or raise exception
          }
        }
        if (previousCellCol < maxColumnIdx) {
          cvsLine.append(((fillValue + csvSeparator) * (maxColumnIdx - previousCellCol - 1)) +
                         fillValue)
        }
        cvsOut.write(cvsLine.toString)
        cvsOut.newLine()
      }
    )
    cvsOut.close()
  }

  def convertExcelSpreadsh2RDD(sc: SparkContext, xlsxFName: String, sheetName: String):
      Unit = {
      // RDD[LinAlgVector] = {

    // we ensure that all vectors inside the generated RDD from the Excel spreadsheet have the
    // same dimension

    println(System.currentTimeMillis + ": Finding max column index in the Excel XLSX")
    val maxColumn = findMaxColumnInExcelSpreadsh(xlsxFName, sheetName)
    println(System.currentTimeMillis + ": Found the max column in Excel spreadsheet: " + maxColumn)

    val csvFullFName = File.createTempFile("excel_xlsx_", ".csv").getAbsolutePath

    println(System.currentTimeMillis + ": Starting conversion of Excel XLSX to CSV text file: " +
            csvFullFName)
    excelSheetToCsv(xlsxFName, sheetName, maxColumn, csvFullFName)
    println(System.currentTimeMillis + ": Finished conversion of Excel XLSX to CSV text file: " +
            csvFullFName)
  }

}
