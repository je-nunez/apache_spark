
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.Tag

import scala.io.Source

import java.io.{BufferedReader, File, FileNotFoundException}

import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector => LinAlgVector}
import org.apache.spark.rdd.RDD

import org.apache.poi.POIXMLException

import excel2rdd.Excel2RDD
import excel2rdd.{ExcelHeaderDiscard, ExcelHeaderExtract, ExcelNoHeader}
import excel2rdd.{ExcelColumnFilter, ExcelDropColumns, ExcelColumnIdentity}


object TagExcelObviousErrors extends Tag("private.tags.ExcelObviousErrors")

object TagExcelNoFilter extends Tag("private.tags.ExcelNoFilter")
object TagExcelRemoveHeader extends Tag("private.tags.RemoveHeaderOnly")
object TagExcelRemove2ndCol extends Tag("private.tags.Remove2ndColOnly")
object TagExcelRemoveThreeCols extends Tag("private.tags.RemoveThreeCols")

object TagExcelFilteringFunc extends Tag("private.tags.ExcelFilteringFunc")


class Excel2Csv extends FunSuite with ShouldMatchers {

  // This is the sample Excel XLSX

  val sampleExcelXlsx = "/sample_excel.xlsx"

  // The right spreadsheet in our Excel file

  val rightSpreadshTab = "test"

  // The directory under which to save the RDD as a CSV

  val saveRddToCsvDir = "/tmp/tempDirectory"      // TODO: do it in a portable manner

  // the filename with real CSV file saved from the RDD

  val realCsvFromRdd = "/tmp/tempDirectory/part-00000"   // using rdd.repartition(1)...

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
  }

  def compareCsvData(realCsvResults: String, expectedCsvResults: String): Boolean = {

    info(s"Comparing contents of '$realCsvResults' with '$expectedCsvResults'.")

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
    val thrownExc = evaluating { excelXlsx.open() } should produce [POIXMLException]

    // This message in the exception may change in the future
    val expectedMsg =
      "org.apache.poi.openxml4j.exceptions.InvalidFormatException: Package should contain a content type part [M1.13]"
    thrownExc.getMessage() should be (expectedMsg)
  }

  test("Converting an unenxisting spreadsheet in an existing Excel XLSX to CSV",
       TagExcelObviousErrors) {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val wrongSpreadshTab = "Non-existing-Spreadsheet-Tab"
    intercept[NullPointerException] {
      val parsedData = excelXlsx.convertExcelSpreadsh2RDD(wrongSpreadshTab, ExcelHeaderExtract,
                                                          ExcelColumnIdentity, sparkContext)
    }
    excelXlsx.close()
  }

  test("Converting an Excel XLSX to CSV, zero filtering of data",
       TagExcelNoFilter, TagExcelFilteringFunc) {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val parsedData = excelXlsx.convertExcelSpreadsh2RDD(rightSpreadshTab, ExcelNoHeader,
                                                        ExcelColumnIdentity, sparkContext)
    info("Done ETL of the input Excel spreadsheet to an Apache Spark RDD.")
    saveRdd2Csv(parsedData, saveRddToCsvDir)
    val res = compareCsvData(realCsvFromRdd, "/parsed_sample_excel_with_header_all_cols.csv")
    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out header row only",
       TagExcelRemoveHeader, TagExcelFilteringFunc) {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val parsedData = excelXlsx.convertExcelSpreadsh2RDD(rightSpreadshTab, ExcelHeaderDiscard,
                                                        ExcelColumnIdentity, sparkContext)
    info("Done ETL of the input Excel spreadsheet to an Apache Spark RDD.")
    saveRdd2Csv(parsedData, saveRddToCsvDir)
    val res = compareCsvData(realCsvFromRdd, "/parsed_sample_excel_no_header_all_cols.csv")
    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out second column only", TagExcelRemove2ndCol,
       TagExcelFilteringFunc) {
    val sampleExcel = getClass.getResourceAsStream(sampleExcelXlsx)
    val excelXlsx = new Excel2RDD(sampleExcel)

    excelXlsx.open()
    val excelDropColumns = new ExcelDropColumns(Array(1))     // drop the second column
    val parsedData = excelXlsx.convertExcelSpreadsh2RDD(rightSpreadshTab, ExcelNoHeader,
                                                        excelDropColumns, sparkContext)
    info("Done ETL of the input Excel spreadsheet to an Apache Spark RDD.")
    saveRdd2Csv(parsedData, saveRddToCsvDir)
    val res = compareCsvData(realCsvFromRdd, "/parsed_sample_excel_with_header_no_2nd_col.csv")
    res should equal (true)
  }

  test("Converting an Excel XLSX to CSV, filtering out second to fourth columns only",
       TagExcelRemoveThreeCols, TagExcelFilteringFunc) {
    pending
  }

  test("Converting an Excel XLSX to CSV, filtering out header row and second column",
       TagExcelFilteringFunc) {
    pending
  }

  test("Converting an Excel XLSX to CSV, filtering out header row and second to fourth columns",
       TagExcelFilteringFunc) {
    pending
  }
}

