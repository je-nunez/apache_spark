// scalastyle:off

package customNYFedBankSCE

import scala.collection.mutable.ArrayBuffer

import excel2rdd.ExcelRowTransform

class NYFedBankSCE extends ExcelRowTransform {

  /**
   * We need to transform some cells in the rows given by the "2014 Housing Survey", from
   * The Center for Microeconomic Data of the Federal Reserve of New York, because some
   * columns give the value of an attribute, and other columns give the classification of
   * that value according to some ranges/categories of values. For example, using the
   * column names in the spreadsheet "Codebook" in the Excel workbook:
   *
   *   column #2, "HQ1_1", is "Value of home in zip code today"
   *   column #3,  "HQ1b", is "Category of value of home in zip code today"
   *
   * so column #3 is a categorization of the raw value in column #2. In similar way:
   *
   *   column "HQH2_1", is "Purchase price of primary residence"
   *   column "HQH2b", is "Category of purchase price of primary residence"
   *
   * and:
   *
   *   column "HQH0", is "Currently own at least one other home besides primary residence" (Yes/No)
   *   column "HQH0a", is "Number of homes owned besides primary residence"
   *
   * We try to leave the cells with the raw values, and drop the cells which have
   * categories of those raw values. If the raw value is empty, then if the category
   * of that raw value is given, then we try to assign a value in that range into the
   * raw value (e.g., the median, although it should be any random value inside that
   * range), and then drop the cell with the categorical value.
   */

  override def transformRow(rowNumber: Int, rowCells: Array[String]): Array[String] = {
    if (rowNumber == 0) {
      // we don't deal with the Excel header row
      rowCells
    } else {
      val newRowCells = ArrayBuffer.empty[String]
      newRowCells ++= rowCells
      val colHQ17 = rowCells.length - 1     // the column HQ17 is the last one in Excel spreadsh
      newRowCells(colHQ17) = transformColHQ17(rowCells(colHQ17))
      newRowCells.toArray
    }
  }

  /**
   * Transform the value of the Excell cells HQ17, with nominal values, to reflect the same
   * distances as the ranges it represents.
   */

  private [this] def transformColHQ17(valueHQ17: String): String = {
    // probably a less error-prone of the mapping of the categories to the ranges is possible,
    // for example, by parsing the Excel spreadsheet with their definitions (the only issue is
    // that it is free-text, so the parsing of those ranges to find its middle point might not
    // be clean either). "0" is a special value which doesn't appear in range, and we receive
    // it when there is no value for HQ17 (ie., when HQ17 is NA).
    val realRangeMiddleMap = Map("0" -> "0", "1" -> "250", "2" -> "750", "3" -> "1500",
                                 "4" -> "3500", "5" -> "7500", "6" -> "15000", "7" -> "25000",
                                 "8" -> "40000", "9" -> "75000", "10" -> "175000",
                                 "11" -> "375000", "12" -> "625000", "13" -> "875000",
                                 "14" -> "1000000")
    realRangeMiddleMap(valueHQ17)
  }

}

