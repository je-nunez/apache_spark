// scalastyle:off

package customNYFedBankSCE

import scala.collection.mutable.ArrayBuffer

import excel2rdd.ExcelRowTransform
import excel2rdd.Excel2RDD

class NYFedBankSCERowTransform(val excelSpreadsh: Excel2RDD) extends ExcelRowTransform {

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
   *
   * (This is not related to giving a uniform value to each entry in the RDD before running the
   *  clustering. This is not done here in the Excel row transform, but in the Spark RDD because
   *  it needs to find the minimum and maximum in a column. See the use of
   *  spark.mllib.feature.StandardScaler in another module.)
   */

  protected [this] val transformationList = Array(
      transformColHQ17 _,
      transformColHQ15 _,
      transformColHQ14 _,
      transformColHQ5b2 _,
      transformColHQ5a _,
      transformColHQR6f _,
      transformColHQR6d _,
      transformColHQR2bnew _

      // HQ6c3 doesn't have a numerical category defined (no distance), but needs to be left as-is
      // to a split tree on this HQ6c3 (but not a K-Means)

   )

  private [this] val mapYears = {
    val tuples = ArrayBuffer.empty[(String, Int)]
    tuples += (("0", 1910))
    tuples += (("1959", 1955))         // "1959" means years before 1960
    for { year <- 1960 to 2014 } {
      tuples += ((year.toString, year))
    }
    tuples.toMap
  }

  override def transformRow(rowNumber: Int, rowCells: Array[String]): Array[String] = {
    if (rowNumber == 0) {
      // we don't deal with the Excel header row
      rowCells
    } else {
      val newRowCells = ArrayBuffer.empty[String]
      newRowCells ++= rowCells

      for (transformation <- transformationList) {
        transformation(newRowCells)
      }

      newRowCells.toArray
    }
  }

  /**
   * Transform the value of the Excell cells HQ17, with nominal values, to reflect the same
   * distances as the ranges it represents.
   */

  private [this] def transformColHQ17(row: ArrayBuffer[String]): Unit = {
    // probably a less error-prone of the mapping of the categories to the ranges is possible,
    // for example, by parsing the Excel spreadsheet with their definitions (the only issue is
    // that it is free-text, so the parsing of those ranges to find its middle point might not
    // be clean either). "0" is a special value which doesn't appear in range, and we receive
    // it when there is no value for HQ17 (ie., when HQ17 is NA).

    val idx = excelSpreadsh.findHeader("HQ17")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 250, "2" -> 750, "3" -> 1500,
                                   "4" -> 3500, "5" -> 7500, "6" -> 15000, "7" -> 25000,
                                   "8" -> 40000, "9" -> 75000, "10" -> 175000,
                                   "11" -> 375000, "12" -> 625000, "13" -> 875000,
                                   "14" -> 1000000)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

  /**
   * Transform the value of the Excell cells HQ15, with nominal values, to reflect the same
   * distances as the ranges it represents.
   */

  private [this] def transformColHQ15(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQ15")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 250, "2" -> 750, "3" -> 1500,
                                   "4" -> 3500, "5" -> 7500, "6" -> 15000, "7" -> 25000,
                                   "8" -> 40000, "9" -> 75000, "10" -> 100000)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

  /**
   * Transform the value of the Excell cells HQ14, with nominal values, to reflect the same
   * distances as the ranges it represents.
   */

  private [this] def transformColHQ14(row: ArrayBuffer[String]): Unit = {

    // There is a typo in the "2014 Housing Survey" Excel workbook for HQ14, from The Center
    // for Microeconomic Data of the Federal Reserve of New York, because it says that the
    // numerical ranges are:
    //     1: [0, 620)
    //     2: [620, 629]
    //     3: [680, 719]
    // so the 2 should be instead [620,679], and not the typo [620,629]

    val idx = excelSpreadsh.findHeader("HQ14")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 310, "2" -> 650, "3" -> 700,
                                   "4" -> 740, "5" -> 770, "6" -> 630)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

  /**
   * Transform the value of the Excell cells HQ5b2, with nominal values, to reflect the same
   * distances as the ranges it represents.
   */

  private [this] def transformColHQ5b2(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQ5b2")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> "0.0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75",
                                   "4" -> "3.25", "5" -> "3.75", "6" -> "4.25", "7" -> "4.75",
                                   "8" -> "5.25", "9" -> "5.75", "10" -> "6.25", "11" -> "6.75",
                                   "12" -> "7.25", "13" -> "7.75", "14" -> "8.25")
      row(idx) = realRangeMiddleMap(row(idx))
    }
  }

  private [this] def transformColHQ5a(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQ5a")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> "0.0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75",
                                   "4" -> "3.25", "5" -> "3.75", "6" -> "4.25", "7" -> "4.75",
                                   "8" -> "5.25", "9" -> "5.75", "10" -> "6.25", "11" -> "6.75",
                                   "12" -> "7.25", "13" -> "7.75", "14" -> "8.25")
      row(idx) = realRangeMiddleMap(row(idx))
    }
  }

  private [this] def transformColHQR6f(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQR6f")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQR6d(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQR6d")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQR2bnew(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQR2bnew")
    if (idx >= 0) {
      val realRangeMiddleMap = Map("0" -> "0.0", "1" -> 3, "2" -> 30, "3" -> 365,
                                   "4" -> 365 * 2, "5" -> 365 * 3, "6" -> 365 * 4)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

}

