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
      transformColCategorical("HQ17") _,
      transformColCategorical("HQ15") _,
      transformColCategorical("HQ14") _,
      transformColCategorical("HQ5b2") _,
      transformColCategorical("HQ5a") _,
      transformColYear("HQR6f") _,
      transformColYear("HQR6d") _,
      transformColCategorical("HQR2bnew") _,
      transformColCategorical("HQR1b") _,
      transformColYear("HQR1a") _,
      transformColYear("HQH11f") _,
      transformColYear("HQH11d") _,
      transformColHQH11b2 _,
      transformColYear("HQH11a") _,
      transformColHQH6a3 _,
      transformColCategorical("HQH6a") _,
      transformColHQH5m3 _,
      transformColYear("HQH5k") _,
      transformColYear("HQH5i") _,
      transformColHQH5c2 _,
      transformColHQH2bnew _,
      transformColHQH2b _,
      transformColCategorical("HQH1b") _,
      transformColYear("HQH1ab2") _,
      transformColYear("HQH1aa") _,
      transformColCategorical("HQH1") _,
      transformColCategorical("HQ4b1") _,
      transformColHQ1_1 _

      // HQ6c3 doesn't have a numerical category defined (no distance), but needs to be left as-is
      // to a split tree on this HQ6c3 (but not a K-Means). The same reason with data columns
      // HQH6d2, HQH6c2, HQH5o, HQH6, HQH5m, HQH5, HQH4a3, HQ4b2, and HQ4b in the NYFed
      // "2014 Housing Survey".

   )

  private [this] val mapYears: Map[String, String] = {
    val tuples = ArrayBuffer.empty[(String, String)]
    tuples += (("0", "1910"))
    tuples += (("1959", "1955"))         // "1959" means years before 1960
    for { year <- 1960 to 2014 } {
      val yearStr = year.toString
      tuples += ((yearStr, yearStr))
    }
    tuples.toMap
  }

  /**
   * These are the middle values of the categories of values for some columns in the "2014 Housing
   * Survey", from The Center for Microeconomic Data of the Federal Reserve of New York.
   *
   * For example, the column in that Excel spreadsheet named "HQ17" has these codes (left) and
   * associated range of values:
   *
   *  Code-received for column "HQ17":
   *  1                                 Less than $500
   *  2                                 $500 to $999
   *  3                                 $1000 to $1999
   *  4                                 $2000 to $4999
   *  5                                 $5000 to $9999
   *  6                                 $10000 to $19999
   *  7                                 $20000 to $29999
   *  8                                 $30000 to $49999
   *  9                                 $50000 to $99999
   *  10                                $100000 to $249999
   *  11                                $250000 to $499999
   *  12                                $500000 to $749999
   *  13                                $750000 to $999999
   *  14                                $1000000 or more
   *
   * This is the input data we receive (column) to the left, but we can't apply K-Means to it
   * because the distances won't have sense, so we convert the values in the left to the middle
   * value of the range in the right. (An alternative to the middle value a small random noise
   * can be added/substracted to the middle value, to be still in the range for the code received.)
   *
   * Note: Probably a less error-prone of the mapping of the categories to the ranges is possible,
   *       for example, by parsing the Excel spreadsheet with their definitions (the only issue is
   *       that it is free-text, so the parsing of those ranges to find its middle point might not
   *       be clean either). "0" is a special value which doesn't appear in range, and we receive
   *       it when there is no value for the cell in the Excel spreadsheet (ie., the cell has NA).
   */

  private [this] val middleValuesOfRangesColumn: Map[String, Map[String, String]] = Map(
    "HQ17" -> Map("0" -> "0", "1" -> "250", "2" -> "750", "3" -> "1500", "4" -> "3500",
                  "5" -> "7500", "6" -> "15000", "7" -> "25000", "8" -> "40000", "9" -> "75000",
                  "10" -> "175000", "11" -> "375000", "12" -> "625000", "13" -> "875000",
                  "14" -> "1000000"),

    "HQ15" -> Map("0" -> "0", "1" -> "250", "2" -> "750", "3" -> "1500", "4" -> "3500",
                  "5" -> "7500", "6" -> "15000", "7" -> "25000", "8" -> "40000", "9" -> "75000",
                  "10" -> "100000"),

        // There is a typo in the "2014 Housing Survey" Excel workbook for HQ14, because it says
        // that the numerical ranges are:
        //     1: [0, 620)
        //     2: [620, 629]
        //     3: [680, 719]
        // so the 2 should be instead [620,679], and not the typo [620,629]
    "HQ14" -> Map("0" -> "0", "1" -> "310", "2" -> "650", "3" -> "700", "4" -> "740",
                  "5" -> "770", "6" -> "630"),

    "HQ5b2" -> Map("0" -> "0.0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75", "4" -> "3.25",
                   "5" -> "3.75", "6" -> "4.25", "7" -> "4.75", "8" -> "5.25", "9" -> "5.75",
                   "10" -> "6.25", "11" -> "6.75", "12" -> "7.25", "13" -> "7.75", "14" -> "8.25"),

    "HQ5a" -> Map("0" -> "0.0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75", "4" -> "3.25",
                  "5" -> "3.75", "6" -> "4.25", "7" -> "4.75", "8" -> "5.25", "9" -> "5.75",
                  "10" -> "6.25", "11" -> "6.75", "12" -> "7.25", "13" -> "7.75", "14" -> "8.25"),

    "HQR2bnew" -> Map("0" -> "0.0", "1" -> "3", "2" -> "30", "3" -> "365",
                      "4" -> s"${365 * 2}", "5" -> s"${365 * 3}", "6" -> s"${365 * 4}"),

        // Here "7" is a little special, more like NA. Very probably this attribute should be
        // treated the same way as column "HQ6c3"
    "HQR1b" -> Map("0" -> "0", "1" -> "1", "2" -> "3", "3" -> "5", "4" -> "7", "5" -> "10",
                   "6" -> "15", "7" -> "0"),

    "HQH6a" -> Map("0" -> "0", "1" -> "3", "2" -> "9", "3" -> "18", "4" -> "30", "5" -> "40"),

    "HQH1b" -> Map("0" -> "0", "1" -> "1", "2" -> "3", "3" -> "5", "4" -> "7", "5" -> "9",
                   "6" -> "12", "7" -> "0"),

    "HQH1" -> (Map("1" -> "0") ++ mapYears),   // HQH1's is almost "mapYears", except "1" -> "0",
                                               // for "1" means "I/We did not buy this residence"

    "HQ4b1" -> Map("0" -> "0", "1" -> "2", "2" -> "12", "3" -> "30", "4" -> "50")

  )


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
   * A curried function to transform a column which has the standard code for the years in the
   * "2014 Housing Survey", from The Center for Microeconomic Data of the Federal Reserve of New
   * York.
   */

  private [this] def transformColYear(columnName: String)(row: ArrayBuffer[String]): Unit = {

    val idx = excelSpreadsh.findHeader(columnName)
    if (idx >= 0) {
      row(idx) = mapYears(row(idx))
    }
  }

  /**
   * A curried function to transform a categorical column in the "2014 Housing Survey", from The
   * Center for Microeconomic Data of the Federal Reserve of New York, to the middle value of
   * its range.
   */

  private [this] def transformColCategorical(columnName: String)(row: ArrayBuffer[String]): Unit =
  {
    require(middleValuesOfRangesColumn.contains(columnName),
            s"The map 'middleValuesOfRangesColumn' must have a definition for key '$columnName'")

    val idx = excelSpreadsh.findHeader(columnName)
    if (idx >= 0) {
      val middleValuesForColumn = middleValuesOfRangesColumn(columnName)
      val originalValue = row(idx)
      val translatedMiddleVal = middleValuesForColumn(originalValue)
      // the new "translatedMiddleVal" corresponding to the input value "originalValue" must be a
      // double, for the K-Means works on double values. (To verify this condition on the map
      // "translatedMiddleVal" for several rows in the input Excel spreadsheet is inefficient,
      // because it could verify the same value multiple times, for different rows in the
      // input Excel spreadsheet.)
      require(translatedMiddleVal.toDouble >= Double.MinValue,
              s"The mapped value for code '$originalValue' in the column key '$columnName' " +
              "must be a double")
      row(idx) = translatedMiddleVal
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

  private [this] def transformColHQH2bnew(row: ArrayBuffer[String]): Unit = {

    val idxHQH2bnew = excelSpreadsh.findHeader("HQH2bnew")
    if (idxHQH2bnew >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 2500, "2" -> 7500, "3" -> 12500, "4" -> 17500,
                                   "5" -> 25000, "6" -> 40000, "7" -> 65000, "8" -> 90000,
                                   "9" -> 125000, "10" -> 175000, "11" -> 350000, "12" -> 625000,
                                   "13" -> 875000)

      val middleValueHQH2bnew = realRangeMiddleMap(row(idxHQH2bnew)).toString

      // the columns "HQH2bnew" and "HQH2new_1" are linearly correlated, so we must leave only one
      // of them in the Apache Spark RDD, otherwise in the K-Means Clustering both columns will
      // increase the weight of their distance by two, making them more important than other
      // columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxHQH2new_1 = excelSpreadsh.findHeader("HQH2new_1")
      if (idxHQH2new_1 >= 0) {
        // the column "HQH2new_1" does exist in the Excel spreadsheet: see if it is empty, and if
        // so, assign to it the middle value of the category "HQH2bnew"
        if (row(idxHQH2new_1) == "" || row(idxHQH2new_1) == "0") {
          row(idxHQH2new_1) = middleValueHQH2bnew
        }
        // in any case, since this column "HQH2new_1" exists, then clear the correlated column
        // "HQH2bnew" which also exists. (See notes above for "HQH11b2" and "HQH11b_1", this
        // case is similar, but the ranges not.)
        row(idxHQH2bnew) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
      } else {
        // the column "HQH2new_1" doesn't exist in all the Excel spreadsheet, only the column
        // "HQH2bnew": just flatten this nominal column "HQH2bnew" into the middle value of the
        // range, so the distance calculation has a little more of meaning
        row(idxHQH2bnew) = middleValueHQH2bnew
      }
    }
  }

  private [this] def transformColHQH2b(row: ArrayBuffer[String]): Unit = {

    val idxHQH2b = excelSpreadsh.findHeader("HQH2b")
    if (idxHQH2b >= 0) {
      val realRangeMiddleMap = Map("0" -> 0, "1" -> 25000, "2" -> 75000, "3" -> 125000,
                                   "4" -> 175000, "5" -> 250000, "6" -> 400000, "7" -> 650000,
                                   "8" -> 900000, "9" -> 1150000)

      val middleValueHQH2b = realRangeMiddleMap(row(idxHQH2b)).toString

      // the columns "HQH2b" and "HQH2_1" are linearly correlated, so we must leave only one
      // of them in the Apache Spark RDD, otherwise in the K-Means Clustering both columns will
      // increase the weight of their distance by two, making them more important than other
      // columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxHQH2_1 = excelSpreadsh.findHeader("HQH2_1")
      if (idxHQH2_1 >= 0) {
        // the column "HQH2_1" does exist in the Excel spreadsheet: see if it is empty, and if
        // so, assign to it the middle value of the category "HQH2b"
        if (row(idxHQH2_1) == "" || row(idxHQH2_1) == "0") {
          row(idxHQH2_1) = middleValueHQH2b
        }
        // in any case, since this column "HQH2_1" exists, then clear the correlated column
        // "HQH2b" which also exists. (See notes above for "HQH11b2" and "HQH11b_1", this
        // case is similar, but the ranges not.)
        row(idxHQH2b) = excelSpreadsh.fillNANullValue.toString   // this value is "0" by default.
      } else {
        // the column "HQH2_1" doesn't exist in all the Excel spreadsheet, only the column
        // "HQH2b": just flatten this nominal column "HQH2b" into the middle value of the
        // range, so the distance calculation has a little more of meaning
        row(idxHQH2b) = middleValueHQH2b
      }
    }
  }

  private [this] def transformColHQ1_1(row: ArrayBuffer[String]): Unit = {

    val idxHQ1_1 = excelSpreadsh.findHeader("HQ1_1")
    val idxHQ1hidden_1 = excelSpreadsh.findHeader("HQ1hidden_1")
    val idxHQ1b = excelSpreadsh.findHeader("HQ1b")

    val realRangeMiddleMapHQ1b =
      Map("0" -> 0, "1" -> 25000, "2" -> 75000, "3" -> 125000, "4" -> 175000, "5" -> 225000,
          "6" -> 275000, "7" -> 325000, "8" -> 375000, "9" -> 425000, "10" -> 475000,
          "11" -> 525000, "12" -> 575000, "13" -> 625000, "14" -> 675000, "15" -> 750000,
          "16" -> 850000, "17" -> 950000, "18" -> 1125000, "19" -> 1375000, "20" -> 1750000,
          "21" -> 2500000, "22" -> 3250000)

    if (idxHQ1_1 >= 0) {

      if (row(idxHQ1_1) == "0" || row(idxHQ1_1) == "0.0" || row(idxHQ1_1) == "") {
        // we need to replace the value of the cell (attribute) HQ1_1 because it is zero, and it
        // have sense the market value of a home to be zero. (In any case, if so, HQ1hidden_1
        // would have the same value of zero, so HQ1_1 would remain in zero.)
        // There are two alternatives to clear the zero value of HQ1_1: first, HQ1hidden_1, if
        // exists, otherwise from HQ1b, if exists.
        if (idxHQ1hidden_1 >= 0) {
          row(idxHQ1_1) = row(idxHQ1hidden_1)
        } else if (idxHQ1b >=0) {
          row(idxHQ1_1) = realRangeMiddleMapHQ1b(row(idxHQ1b)).toString
        }
      }

      // Clear the value of the related attributes HQ1hidden_1 and HQ1b, if they exist.
      if (idxHQ1hidden_1 >= 0) {
        row(idxHQ1hidden_1) = excelSpreadsh.fillNANullValue.toString
      }
      if (idxHQ1b >= 0) {
        row(idxHQ1b) = excelSpreadsh.fillNANullValue.toString
      }
    } else if (idxHQ1hidden_1 >= 0 && idxHQ1b >= 0) {
      // HQ1_1 does not exist for the entire dataset, but HQ1hidden_1 and HQ1b do: we let the
      // K-Means run on HQ1hidden_1 instead on the default HQ1_1
      if (row(idxHQ1hidden_1) == "0" || row(idxHQ1hidden_1) == "0.0" ||
          row(idxHQ1hidden_1) == "") {
        row(idxHQ1_1) = realRangeMiddleMapHQ1b(row(idxHQ1b)).toString
      }
      // Clear the value of the related attribute HQ1b, if it exists
      if (idxHQ1b >= 0) {
        row(idxHQ1b) = excelSpreadsh.fillNANullValue.toString
      }
    } else if (idxHQ1b >= 0) {
      // HQ1_1 and HQ1hidden_1 do not exist for the entire dataset, but HQ1b does: we let the
      // K-Means run on HQ1b
      row(idxHQ1b) = realRangeMiddleMapHQ1b(row(idxHQ1b)).toString
    }

  }

}
