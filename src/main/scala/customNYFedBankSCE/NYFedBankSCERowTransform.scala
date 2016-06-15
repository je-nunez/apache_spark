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
      transformColHQR2bnew _,
      transformColHQR1b _,
      transformColHQR1a _,
      transformColHQH11f _,
      transformColHQH11d _,
      transformColHQH11b2 _,
      transformColHQH11a _,
      transformColHQH6a3 _,
      transformCoHQH6a _,
      transformColHQH5m3 _,
      transformColHQH5k _,
      transformColHQH5i _,
      transformColHQH5c2 _

      // HQ6c3 doesn't have a numerical category defined (no distance), but needs to be left as-is
      // to a split tree on this HQ6c3 (but not a K-Means). The same reason with data columns
      // HQH6d2, HQH6c2, HQH5o, HQH6, HQH5m, HQH5 and HQH4a3 in the NYFed "2014 Housing Survey".

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

  private [this] def transformColHQR1b(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQR1b")
    if (idx >= 0) {
      // Here "7" is a little special, more like NA. Very probably this attribute should be
      // treated the same way as column "HQ6c3"
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 1, "2" -> 3, "3" -> 5,
                                   "4" -> 7, "5" -> 10, "6" -> 15, "7" -> 0)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

  private [this] def transformColHQR1a(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQR1a")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH11f(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH11f")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH11d(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH11d")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH11b2(row: ArrayBuffer[String]): Unit = {

    val idxHQH11b2 = excelSpreadsh.findHeader("HQH11b2")
    if (idxHQH11b2 >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 12500, "2" -> 37500, "3" -> 75000,
                                   "4" -> 125000, "5" -> 175000, "6" -> 250000, "7" -> 400000,
                                   "8" -> 650000, "9" -> 850000)
      val middleValueHQH11b2 = realRangeMiddleMap(row(idxHQH11b2)).toString
      // the columns "HQH11b_1" and "HQH11b2" are linearly correlated in the "2014 Housing Survey",
      // from The Center for Microeconomic Data of the Federal Reserve of New York, so we must
      // leave only one of them in the Apache Spark RDD, otherwise in the K-Means Clustering
      // both columns will increase the weight of their distance by two, making them more important
      // than other columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxHQH11b_1 = excelSpreadsh.findHeader("HQH11b_1")
      if (idxHQH11b_1 >= 0) {
        // the column "HQH11b_1" does exist in the Excel spreadsheet: see if it is empty, and if
        // so, assign to it the middle value of the category "HQH11b2"
        if (row(idxHQH11b_1) == "" || row(idxHQH11b_1) == "0") {
          row(idxHQH11b_1) = middleValueHQH11b2
        }
        // in any case, since this column "HQH11b_1" exists, then clear the correlated column
        // "HQH11b2" which also exists. This way, we are clearing the correlation and making the
        // K-Means clustering unbiased again.
        row(idxHQH11b2) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
        // An alternative to setting the value to "0" of the entry HQH11b2 in the row vector, would
        // be to delete this entry HQH11b2 in the vector (via a "row.remove(idxHQH11b2)" in Scala)
        // but then the mapping between the header names array (the protected array
        // "excelSpreadsh.header" used inside excelSpreadsh.findHeader(...)) and the vector data is
        // shifted by one, because the column HQH11b2 was deleted in each (all) of the data
        // vectors, but not in header names array. It is easier just to set the value of HQH11b2 to
        // "0" in the data vector, just clearing the correlation with the other column HQH11b_1,
        // than to wholy delete this entry HQH11b2 in the data vector.
      } else {
        // the column "HQH11b_1" doesn't exist in all the Excel spreadsheet, only the column
        // "HQH11b2": just flatten this nominal column "HQH11b2" into the middle value of the
        // range, so the distance calculation has a little more of meaning
        row(idxHQH11b2) = middleValueHQH11b2
      }
    }
  }

  private [this] def transformColHQH11a(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH11a")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH6a3(row: ArrayBuffer[String]): Unit = {

    // column HQH6a3 is the sign of the magnitude given in HQH6a3part2_1
    val idxHQH6a3 = excelSpreadsh.findHeader("HQH6a3")
    val idxHQH6a3part2_1 = excelSpreadsh.findHeader("HQH6a3part2_1")

    if (idxHQH6a3 >= 0 && idxHQH6a3part2_1 >= 0) {
      // both columns HQH6a3 and HQH6a3part2_1 exist

      if (row(idxHQH6a3) == "3") {
        // then the sign of the absolute magnitude should be negative
        row(idxHQH6a3part2_1) = "-" + row(idxHQH6a3part2_1)
      }

      // in any case, since both columns HQH6a3 and HQH6a3part2_1 exist and HQH6a3 is just the
      // sign of the magnitude in HQH6a3part2_1 (assigned above), then we can clear the sign
      // in HQH6a3, since it has already been represented in HQH6a3part2_1 (the only doubt
      // should be if the value of HQH6a3 is "4", which means "Don't know", but it is difficult
      // to see how this "4" can have a meaningful K-Means distance with other values of this
      // column HQH6a3)

      row(idxHQH6a3) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
    }
  }

  private [this] def transformCoHQH6a(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH6a")
    if (idx >= 0) {
      // Here "7" is a little special, more like NA. Very probably this attribute should be
      // treated the same way as column "HQ6c3"
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 3, "2" -> 9, "3" -> 18, "4" -> 30,
                                   "5" -> 40)
      row(idx) = realRangeMiddleMap(row(idx)).toString
    }
  }

  private [this] def transformColHQH5m3(row: ArrayBuffer[String]): Unit = {

    val idxHQH5m3 = excelSpreadsh.findHeader("HQH5m3")
    if (idxHQH5m3 >= 0) {
      val realRangeMiddleMap = Map("0" -> "0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75",
                                   "4" -> "3.25", "5" -> "3.75", "6" -> "4.25", "7" -> "4.75",
                                   "8" -> "5.25", "9" -> "5.75", "10" -> "6.25", "11" -> "6.75",
                                   "12" -> "7.25", "13" -> "7.75", "14" -> "8.25")

      val middleValueHQH5m3 = realRangeMiddleMap(row(idxHQH5m3))

      // the columns "HQH5m2_1" and "HQH5m3" are linearly correlated, so we must leave only one of
      // them in the Apache Spark RDD, otherwise in the K-Means Clustering both columns will
      // increase the weight of their distance by two, making them more important than other
      // columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxHQH5m2_1 = excelSpreadsh.findHeader("HQH5m2_1")
      if (idxHQH5m2_1 >= 0) {
        // the column "HQH5m2_1" does exist in the Excel spreadsheet: see if it is empty, and if
        // so, assign to it the middle value of the category "HQH5m3"
        if (row(idxHQH5m2_1) == "" || row(idxHQH5m2_1) == "0") {
          row(idxHQH5m2_1) = middleValueHQH5m3
        }
        // in any case, since this column "HQH5m2_1" exists, then clear the correlated column
        // "HQH5m3" which also exists. (See notes above for "HQH11b2" and "HQH11b_1", this
        // case is similar, but the ranges not.)
        row(idxHQH5m3) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
      } else {
        // the column "HQH5m2_1" doesn't exist in all the Excel spreadsheet, only the column
        // "HQH5m3": just flatten this nominal column "HQH5m3" into the middle value of the
        // range, so the distance calculation has a little more of meaning
        row(idxHQH5m3) = middleValueHQH5m3
      }
    }
  }

  private [this] def transformColHQH5k(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH5k")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH5i(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader("HQH5i")
    if (idx >= 0) {
      row(idx) = mapYears(row(idx)).toString
    }
  }

  private [this] def transformColHQH5c2(row: ArrayBuffer[String]): Unit = {

    val idxHQH5c2 = excelSpreadsh.findHeader("HQH5c2")
    if (idxHQH5c2 >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 250, "2" -> 750, "3" -> 1250, "4" -> 1750,
                                   "5" -> 2250, "6" -> 2750, "7" -> 3250, "8" -> 3750,
                                   "9" -> 4250, "10" -> 4750, "11" -> 5250, "12" -> 5750,
                                   "13" -> 6250)

      val middleValueHQH5c2 = realRangeMiddleMap(row(idxHQH5c2)).toString

      // the columns "HQH5b" and "HQH5c2" are linearly correlated, so we must leave only one of
      // them in the Apache Spark RDD, otherwise in the K-Means Clustering both columns will
      // increase the weight of their distance by two, making them more important than other
      // columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxHQH5b = excelSpreadsh.findHeader("HQH5b")
      if (idxHQH5b >= 0) {
        // the column "HQH5b" does exist in the Excel spreadsheet: see if it is empty, and if
        // so, assign to it the middle value of the category "HQH5c2"
        if (row(idxHQH5b) == "" || row(idxHQH5b) == "0") {
          row(idxHQH5b) = middleValueHQH5c2
        }
        // in any case, since this column "HQH5b" exists, then clear the correlated column
        // "HQH5c2" which also exists. (See notes above for "HQH11b2" and "HQH11b_1", this
        // case is similar, but the ranges not.)
        row(idxHQH5c2) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
      } else {
        // the column "HQH5b" doesn't exist in all the Excel spreadsheet, only the column
        // "HQH5c2": just flatten this nominal column "HQH5c2" into the middle value of the
        // range, so the distance calculation has a little more of meaning
        row(idxHQH5c2) = middleValueHQH5c2
      }
    }
  }


}

