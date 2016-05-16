
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.Tag

import scala.io.Source

import java.io.{BufferedReader, File, FileNotFoundException}

import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector}
import org.apache.spark.rdd.RDD

import org.apache.poi.POIXMLException

import excel2rdd.Excel2RDD
import excel2rdd.{ExcelRowFilter, ExcelHeaderDiscard, ExcelHeaderExtract, ExcelNoHeader}
import excel2rdd.{ExcelColumnFilter, ExcelDropColumns, ExcelColumnIdentity}


object TagExcelObviousErrors extends Tag("private.tags.ExcelObviousErrors")

object TagExcelFilteringFunc extends Tag("private.tags.Excel.Filter")

object TagExcelDoFilterHeader extends Tag("private.tags.Excel.Filter.Header.DoFilter")
object TagExcelNoFilterHeader extends Tag("private.tags.Excel.Filter.Header.NoFilter")

object TagExcelNoFilterColumns extends Tag("private.tags.Excel.Filter.Columns.NoFilter")
object TagExcelDoFilter2ndColm extends Tag("private.tags.Excel.Filter.Columns.Filter2d")
object TagExcelDoFilter2t4Cols extends Tag("private.tags.Excel.Filter.Columns.Filter2t4")


class TestExcel2Rdd extends FunSuite with Matchers {

  // This is the sample Excel XLSX

  val sampleExcelXlsx = "/sample_excel.xlsx"

  // The right spreadsheet in our sample Excel file

  val rightSpreadshTab = "test"

  // The right values of the header row in our sample Excel file

  val rightSpreadshHeader = Array("headerCol1", "headerCol2", "headerCol3", "headerCol4",
                                  "headerCol5", "headerCol6", "headerCol7")

  // The directory under which to save the RDD as a CSV

  val saveRddToCsvDir = "/tmp/tempDirectory"      // TODO: do it in a portable manner, and for
                              // parallel test execution (disabled in the "build.sbt" SBT file,
                              // http://www.scala-sbt.org/0.13/docs/Parallel-Execution.html

  // the filename with the real CSV file saved from the RDD

  val realCsvFromRdd = saveRddToCsvDir + "/part-00000"   // using rdd.repartition(1)...: see below

  lazy val sparkContext = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Test")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)
    info("Just created the Apache Spark Context.")

    sc
  }

  def saveRdd2Csv(rdd: RDD[LinAlgVector], outputBaseDir: String): Unit = {

    FileUtils.deleteQuietly(new File(outputBaseDir))

    rdd.repartition(1).saveAsTextFile(outputBaseDir)
    info("Done saving the parsed Spark RDD back to a single CSV file.")
  }

  def compareCsvData(realCsvResults: String, expectedCsvResults: String): Boolean = {

    info(s"Comparing contents of '$realCsvResults' with the expected '$expectedCsvResults'.")

    val realLines = Source.fromFile(realCsvResults).getLines
    val expectedLines =
      Source.fromInputStream(getClass.getResourceAsStream(expectedCsvResults)).getLines

    // compare the two BufferedLineIterator's

    realLines.zip(expectedLines).forall(pair => pair._1 == pair._2) &&
      (realLines.length == expectedLines.length)

  }

  test("Converting an unenxisting Excel XLSX to CSV", TagExcelObviousErrors) {

    val nonExistingXlsx = File.createTempFile("non-existing-excel-file-", ".xlsx")
    val excelXlsx = new Excel2RDD(nonExistingXlsx.getAbsolutePath)

    // This message given in the Apache POI exception may change in the future
    val expectedMsg =
      "org.apache.poi.openxml4j.exceptions.InvalidFormatException: Package should contain a content type part [M1.13]"

    the [POIXMLException] thrownBy { excelXlsx.open() } should have message expectedMsg
  }

  test("Converting an unenxisting spreadsheet in an existing Excel XLSX to CSV",
       TagExcelObviousErrors) {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val wrongSpreadshTab = rightSpreadshTab + "_a_surpluous_wrong_suffix"    // make it wrong
    intercept[NullPointerException] {
      val parsedData = excelXlsx.convertExcelSpreadsh2RDD(wrongSpreadshTab, ExcelHeaderExtract,
                                                          ExcelColumnIdentity, sparkContext)
    }
    excelXlsx.close()
  }


  def stdTestExcel2RddWithFilters(rowFilter: ExcelRowFilter, columnFilter: ExcelColumnFilter,
                                  expectedRddAsCsvResult: String,
                                  checkState: Excel2RDD => Boolean): Boolean = {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val parsedData = excelXlsx.convertExcelSpreadsh2RDD(rightSpreadshTab, rowFilter, columnFilter,
                                                        sparkContext)
    excelXlsx.close()
    info("Done ETL of the input Excel spreadsheet to an Apache Spark RDD.")
    saveRdd2Csv(parsedData, saveRddToCsvDir)

    if (compareCsvData(realCsvFromRdd, expectedRddAsCsvResult)) {
      // the comparison of the contents from the realCsvFromRdd is the same as the expected
      // contents in expectedRddAsCsvResult. Check the state of the object itself
      checkState(excelXlsx)
    } else {
      // the CSV contents from the actual Spark RDD and the expected contents were different
      false
    }
  }

  test("Converting an Excel XLSX to CSV, zero filtering of data",
       TagExcelNoFilterHeader, TagExcelNoFilterColumns, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelNoHeader, ExcelColumnIdentity,
                                          "/parsed_sample_excel_with_header_all_cols.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out header row only",
       TagExcelDoFilterHeader, TagExcelNoFilterColumns, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelHeaderDiscard, ExcelColumnIdentity,
                                          "/parsed_sample_excel_no_header_all_cols.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out second column only",
       TagExcelNoFilterHeader, TagExcelDoFilter2ndColm, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelNoHeader, new ExcelDropColumns(Array(1)),
                                          "/parsed_sample_excel_with_header_no_2nd_col.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out second to fourth columns only",
       TagExcelNoFilterHeader, TagExcelDoFilter2t4Cols, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelNoHeader, new ExcelDropColumns(Array(1, 2, 3)),
                                          "/parsed_sample_excel_with_header_no_2nd_to_4th_cols.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out header row and second column",
       TagExcelDoFilterHeader, TagExcelDoFilter2ndColm, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelHeaderDiscard, new ExcelDropColumns(Array(1)),
                                          "/parsed_sample_excel_no_header_no_2nd_col.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out header row and second to fourth columns",
       TagExcelDoFilterHeader, TagExcelDoFilter2t4Cols, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelHeaderDiscard,
                                          new ExcelDropColumns(Array(1, 2, 3)),
                                          "/parsed_sample_excel_no_header_no_2nd_to_4th_cols.csv",
                                          (e: Excel2RDD) => true)

    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out header row only and saving it internally",
       TagExcelDoFilterHeader, TagExcelNoFilterColumns, TagExcelFilteringFunc) {

    val res = stdTestExcel2RddWithFilters(ExcelHeaderExtract, ExcelColumnIdentity,
                                          "/parsed_sample_excel_no_header_all_cols.csv",
                                          (e: Excel2RDD) => {
                                             rightSpreadshHeader.zipWithIndex forall {
                                               case (expectedHdrVal, hrdIdx) =>
                                                 e.getHeader(hrdIdx) == expectedHdrVal
                                             }
                                          })

    res should equal (true)
  }

}

