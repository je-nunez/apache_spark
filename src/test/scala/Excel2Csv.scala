
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.Tag

import scala.io.Source

import java.io.FileNotFoundException

import org.apache.spark.{SparkConf, SparkContext}


import excel2rdd.Excel2RDD
import excel2rdd.{ExcelHeaderExtract, ExcelNoHeader}
import excel2rdd.{ExcelColumnFilter, ExcelDropColumns, ExcelColumnIdentity}


object ExcelObviousErrors extends Tag("me.tags.ExcelObviousErrors")

object ExcelFilteringFunc extends Tag("me.tags.ExcelFilteringFunc")


class Excel2Csv extends FunSuite with ShouldMatchers {

  def getSparkContext: SparkContext = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Test")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)
    info("Just created a new Apache Spark Context.")

    sc
  }

  test("Converting an unenxisting Excel XLSX to CSV", ExcelObviousErrors) {

    val sc = getSparkContext

    val excelXlsx = new Excel2RDD("/tmp/non-existing-excel-file.xlsx")     // TODO: improve this
    val thrownExc = evaluating { excelXlsx.open() } should produce [FileNotFoundException]
    thrownExc.getMessage() should be ("/tmp/non-existing-excel-file.xlsx (No such file or directory)")
  }

  test("Converting an unenxisting spreadsheet in an existing Excel XLSX to CSV", ExcelObviousErrors) {
    pending
  }

  test("Converting an Excel XLSX to CSV, zero filtering of data", ExcelFilteringFunc) {
    // TODO
    // val excelFile = Source.fromURL(getClass.getResource("/excel.xlsx"))
    pending
  }

  test("Converting an Excel XLSX to CSV, filtering out header row only", ExcelFilteringFunc) {
    pending
  }

  test("Converting an Excel XLSX to CSV, filtering out second column only", ExcelFilteringFunc) {
    pending
  }

  test("Converting an Excel XLSX to CSV, filtering out header row and second column",
       ExcelFilteringFunc) {
    pending
  }

}

