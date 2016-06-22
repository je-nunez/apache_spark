// scalastyle:off

package excel2rdd

import scala.annotation.switch
import scala.collection.immutable.StringOps
import scala.util.Try
import scala.util.matching._

import java.io.{File, InputStream, FileInputStream, FileWriter, BufferedWriter}

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
// spreadsheets with other schemas. More details in the comments below, specifically the
// Cell.CELL_TYPE_ conversions, and <type>, RDD[<type>], of the vectors in the new RDD.

class Excel2RDD(

     // we use the Java class InputStream instead of scala.io.Source, BufferedSource, etc, because
     // Apache POI works at the end with Java classes to read the Microsoft Office documents

     val xlsxInput: InputStream
  ) {

  def this(xlsxFName: String) {
    // an optional constructor receiving the (directory and) file name of the Excel XLSX
    this(new FileInputStream(xlsxFName))
  }

  protected[this] var xlsWbk: XSSFWorkbook = null

  // These values below are used in closures inside Spark, so they should be "final" if they want
  // to be referred inside Spark. (Another solution would be to have a local copy of them before
  // calling the Spark methods, referring to their local copies inside.) Otherwise you will get:
  //      Exception ... org.apache.spark.SparkException: Task not serializable

  /**
   * Numeric string for filling empty cells (ie., to fill NA, or null).
   *
   * @note There is the issue with what specific numeric value to assign to NA in the input
   * source for the newly generared LinAlgVector, because LinAlgVector only contains doubles:
   * @see https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/linalg/Vector.html
   *)
   */

  final val fillNANullValue = "0"        // string for filling empty cells (ie., to fill NA, or null)

  final val csvSeparator = ","

  /**
   * Header in the Excel spreadsheed.
   */

  protected[this] var header: Option[Array[String]] = None

  /**
   * The underlying, temporary csvFullFName that was generated from this Excel spreadsheet
   */

  protected[this] var csvFullFName: String = ""


  def open(): Unit = {
    xlsWbk = new XSSFWorkbook(xlsxInput)
  }

  def close(): Unit = {
    if (xlsWbk != null) {
      xlsWbk.close()
    }
  }

  def getHeader(i: Int): String = {
    header match {
      case Some(arr) => if (i >= 0 && i < arr.length) arr(i) else ""
      case None => ""
    }
  }

  def findHeader(hdrName: String): Int = {
    header match {
      case Some(arr) => arr.indexOf(hdrName)
      case None => -1
    }
  }

  def getCsvFileName: String = csvFullFName


  def iterExcelRows(sheetName: String, rowFunction: XSSFRow => Unit): Unit = {
    val xlsSheet = xlsWbk.getSheet(sheetName)

    val rows = xlsSheet.rowIterator()    // get an iterator over the rows

    while (rows.hasNext()) {
      rowFunction(rows.next().asInstanceOf[XSSFRow])
    }
    xlsWbk.close()

  }

  def findMaxColumnInExcelSpreadsh(sheetName: String): Int = {

    // This method is necessary if we want to enforce that the vectors inside the new RDD have all
    // the same dimension, because Excel does not need that all rows have the same number of
    // columns.

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

    // This method is just a pretty printer of floats into the CSV, to avoid exponential notation
    // or 5.350000 with extra non-significant '0's to the right

    val directConversion = "%f".format(d)

    if (directConversion.indexOf('.') >= 1) {
      directConversion.reverse.dropWhile(_ == '0').dropWhile(_ == '.').reverse
      // dropWhile seems slightly faster than tail-recursion to remove non-significant '0'
      // generated when reading the floating-points in the source Excel cells
    } else {
      directConversion
    }
  }

  def fillEmptyCells(previousVisitedCol: Int, currVisitCol: Int, colFilter: ExcelColumnFilter):
      String = {

    val strPreffix = if (previousVisitedCol > -1) csvSeparator else ""
    if (previousVisitedCol == currVisitCol - 1) {
      strPreffix    // this if-condition is the trivial case
    } else {
      // previousVisitedCol < currVisitCol - 1, so there were some cells missing in Excel spreadsheet
      // We can either fill them with fillNANullValue, or discard any cell if it was supposed to be
      // filtered-out explicitly by the colFilter
      var filling = new StringBuilder(8 * 1024)
      for { jumpedOverCol <- previousVisitedCol+1 until currVisitCol } {
        colFilter(jumpedOverCol, ( fillNANullValue + csvSeparator )) match {
          case Some(nonFilteredValue) => filling.append(nonFilteredValue)
          case _ =>
        }
      }
      strPreffix + filling.toString
    }
  }

  // A very simple converter of an Excel XLSX spreadsheet to a CSV text file. In particular, it
  // fills empty cells with fillNANullValue. Empty rows in the spreadsheet are omitted (nothing
  // is outputted in the CSV for them), for we want to convert at the end to a Spark RDD and
  // completely empty rows in a RDD don't have too much meaning -except to increase the number
  // of rows in the RDD and those measures that depend on the number of rows, like mean, standard
  // deviation, etc, where the number of rows appears in the denominator.
  // (A subsequent version will omit the intermediate CSV of the Excel spreadsheet)
  // There are other, more complete Excel XLSX - to -> CSV converters as examples of Apache POI:
  //    https://poi.apache.org/spreadsheet/examples.html

  protected[this] def excelSheetToCsv(sheetName: String, maxColumnIdx: Int,
      rowFilter: ExcelRowFilter, colFilter: ExcelColumnFilter, rowTransform: ExcelRowTransform,
      csvFName: String): Unit = {

    val cvsOut = new BufferedWriter(new FileWriter(csvFName))
    val cvsLine = new StringBuilder(8 * 1024)

    iterExcelRows(sheetName,
      (row: XSSFRow) => {
        cvsLine.clear()
        val currentRow = row.getRowNum
        val cells = row.cellIterator    // get an iterator over the cells in this row
        var previousCellCol: Int = -1

        while (cells.hasNext)
        {
          val cell = cells.next.asInstanceOf[XSSFCell]
          val currentCol = cell.getColumnIndex

          var valueStr = ""
          (cell.getCellType: @switch) match {
            // As a matter of fact, since our RDD happens to be RDD[LinAlgVector], we only expect
            // the Excel cells to be Cell.CELL_TYPE_NUMERIC (other RDD[<types>] could be freer)
            case Cell.CELL_TYPE_STRING => valueStr = cell.getStringCellValue
            case Cell.CELL_TYPE_NUMERIC => valueStr = convertDouble(cell.getNumericCellValue)
            case Cell.CELL_TYPE_BOOLEAN => valueStr = cell.getBooleanCellValue.toString
            case _ => valueStr = "Unknown value at Row: " + (currentRow + 1) +
                                 " Column: " + (currentCol + 1)     // or raise exception
          }
          colFilter(currentCol, valueStr) match {
            case Some(s) => {
              cvsLine.append(fillEmptyCells(previousCellCol, currentCol, colFilter))
              cvsLine.append(s)
              previousCellCol = currentCol
            }
            case _ =>
          }
        }
        if (previousCellCol < maxColumnIdx) {
          cvsLine.append((csvSeparator + fillNANullValue) * (maxColumnIdx - previousCellCol))
        }
        // TODO: use the rowTransform parameter (which transforms a row; the filter may drop it)
        rowFilter(currentRow, cvsLine.toString,
                  line => {
                    val cellValues = line.split(csvSeparator)
                    val newValues = rowTransform(currentRow, cellValues)
                    cvsOut.write(newValues.mkString(csvSeparator))
                    cvsOut.newLine()
                  },
                  line => { header = Some(line.split(csvSeparator)) })
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
             token => Try(token.toDouble).getOrElse(fillNANullValue.toDouble)
           )
      )
    ).cache()

    // println("Data parsed. Class " + parsedData.getClass.getName)
    parsedData
  }


  def convertExcelSpreadsh2RDD(sheetName: String, rowFilter: ExcelRowFilter,
      colFilter: ExcelColumnFilter, rowTransform: ExcelRowTransform, sc: SparkContext):
      RDD[LinAlgVector] = {

    // we ensure that all vectors inside the generated RDD from the Excel spreadsheet have the
    // same dimension

    // println(System.currentTimeMillis + ": Finding max column index in the Excel XLSX spreadsh")
    val maxColumn = findMaxColumnInExcelSpreadsh(sheetName)
    // println(System.currentTimeMillis + ": Found the max column: " + maxColumn)

    var newRDD: RDD[LinAlgVector] = null
    val csvFile = File.createTempFile("excel_xlsx_", ".csv")
    csvFile.deleteOnExit()
    try {
      val tempCsvFName = csvFile.getAbsolutePath

      println(System.currentTimeMillis + ": Starting conversion of Excel XLSX to CSV text file: " +
              tempCsvFName)
      excelSheetToCsv(sheetName, maxColumn, rowFilter, colFilter, rowTransform, tempCsvFName)
      println(System.currentTimeMillis + ": Finished conversion of Excel XLSX to CSV text file: " +
              tempCsvFName)

      newRDD = convertCsv2RDD(tempCsvFName, sc)
      csvFullFName = csvFile.getAbsolutePath
    } finally {
      // FileUtils.deleteQuietly(csvFile)
      //   TODO: FIX:
      //      can't delete it immediately in this thread, because Spark is still reading it in
      //      another thread. That's why the csvFile.deleteOnExit() above, to delete it on exit.
      //      This is an issue though inside Unix-style daemons or Windows system services.
    }

    newRDD
  }

}

