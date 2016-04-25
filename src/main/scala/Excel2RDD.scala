// scalastyle:off

package excel2rdd

import scala.annotation.switch
import scala.collection.immutable.StringOps
import scala.util.Try
import scala.util.matching._

import java.io.{File, FileInputStream, FileWriter, BufferedWriter}

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector, Vectors}
import org.apache.spark.rdd.RDD

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFCell
import org.apache.poi.ss.usermodel.Cell

// This is a very simple converter from an Excel spreadsheet to a Spark RDD, using an intermediate
// CSV file. It is not a general converter, for it is more useful when the cells in the Excel
// spreadsheet contains only numbers or are empty: with minor modifications it can be used for
// spreadsheets with other schemas. More details in the comments below.

class Excel2RDD(
     val xlsxFName: String
  ) {

  protected[this] var xlsWbk: XSSFWorkbook = null

  // These values are used in closures inside Spark, so they should be "final" if they
  // wanted to be referred inside Spark. (Another solution would be to have a local
  // copy of them before calling the Spark methods, using their copies inside.) Otherwise
  // you will get:
  //   Exception ... org.apache.spark.SparkException: Task not serializable

  final val fillValue = "0"        // string for filling empty cells (ie., to fill NA)
  final val csvSeparator = ","

  def open(): Unit = {
    val excelFileToRead = new FileInputStream(xlsxFName)
    xlsWbk = new XSSFWorkbook(excelFileToRead)
  }

  def close(): Unit = {
    if (xlsWbk != null) {
      xlsWbk.close()
    }
  }

  def iterExcelRows(sheetName: String, rowFunction: XSSFRow => Unit): Unit = {
    val xlsSheet = xlsWbk.getSheet(sheetName)

    val rows = xlsSheet.rowIterator()    // get an iterator over the rows

    while (rows.hasNext()) {
      rowFunction(rows.next().asInstanceOf[XSSFRow])
    }
    xlsWbk.close()

  }

  def findMaxColumnInExcelSpreadsh(sheetName: String): Int = {

    var maxCol: Int = -1

    iterExcelRows(sheetName,
                  (row: XSSFRow) => {
                    val maxColRow = row.getLastCellNum - 1
                    if (maxColRow > maxCol) { maxCol = maxColRow }
                  }
    )
    maxCol
  }

  protected[this] def convertDouble(d: Double): String = {

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

  protected[this] def excelSheetToCsv(sheetName: String, maxColumnIdx: Int, csvFName: String):
      Unit = {

    val cvsOut = new BufferedWriter(new FileWriter(csvFName))
    val cvsLine = new StringBuilder(8 * 1024)

    iterExcelRows(sheetName,
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
          cvsLine.append((csvSeparator + fillValue) * (maxColumnIdx - previousCellCol))
        }
        cvsOut.write(cvsLine.toString)
        cvsOut.newLine()
      }
    )
    cvsOut.close()
  }

  def convertCsv2RDD(csvFName: String, sc: SparkContext): RDD[LinAlgVector] = {

    println(System.currentTimeMillis + ": Loading CSV file: " + csvFName)
    val data = sc.textFile(csvFName)
    println(System.currentTimeMillis + ": CSV file loaded: " + csvFName)
    // println("\nNumber of elements in the Resilient Distributed Dataset: " + data.count)

    val parsedData = data.map(
      s => Vectors.dense(s.split(csvSeparator).map(
             token => Try(token.toDouble).getOrElse(fillValue.toDouble)
           )
      )
    ).cache()

    println("Data parsed. Class " + parsedData.getClass.getName)
    parsedData
  }


  def convertExcelSpreadsh2RDD(sheetName: String, sc: SparkContext):
      RDD[LinAlgVector] = {

    // we ensure that all vectors inside the generated RDD from the Excel spreadsheet have the
    // same dimension

    println(System.currentTimeMillis + ": Finding max column index in the Excel XLSX")
    val maxColumn = findMaxColumnInExcelSpreadsh(sheetName)
    println(System.currentTimeMillis + ": Found the max column in Excel spreadsheet: " + maxColumn)

    var newRDD: RDD[LinAlgVector] = null
    val csvFile = File.createTempFile("excel_xlsx_", ".csv")
    try {
      val csvFullFName = csvFile.getAbsolutePath

      println(System.currentTimeMillis + ": Starting conversion of Excel XLSX to CSV text file: " +
              csvFullFName)
      excelSheetToCsv(sheetName, maxColumn, csvFullFName)
      println(System.currentTimeMillis + ": Finished conversion of Excel XLSX to CSV text file: " +
              csvFullFName)

      newRDD = convertCsv2RDD(csvFullFName, sc)
    } finally {
      // FileUtils.deleteQuietly(csvFile)
    }

    newRDD
  }

}
