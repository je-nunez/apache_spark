
import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.io.Source

object CsvOnly extends Tag("me.tags.TestTillCsvOnly")


class Excel2Csv extends FunSuite {

  test("Converting an Excel XLSX to CSV") {
    // TODO
    // val excelFile = Source.fromURL(getClass.getResource("/excel.xlsx"))
    assertResult(1) { 1 + 0 }
  }

}

