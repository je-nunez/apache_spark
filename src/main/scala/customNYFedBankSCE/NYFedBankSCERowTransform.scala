// scalastyle:off

// This module implements a translation of the "2014 Housing Survey", from The Center for
// Microeconomic Data of the Federal Reserve of New York, to continuous real-values, so
// explicit literals are necessary here. (In a future version, this translation will
// reside in a separate resource file in this same JAR archive. Meanwhile:
// scalastyle:off multiple.string.literals

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

  protected[this] val transformationList = Array(
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
      transformRawCol4CategCol("HQH11b_1", "HQH11b2") _,
      transformColYear("HQH11a") _,
      transformColHQH6a3 _,
      transformColCategorical("HQH6a") _,
      transformRawCol4CategCol("HQH5m2_1", "HQH5m3") _,
      transformColYear("HQH5k") _,
      transformColYear("HQH5i") _,
      transformRawCol4CategCol("HQH5b", "HQH5c2") _,
      transformRawCol4CategCol("HQH2new_1", "HQH2bnew") _,
      transformRawCol4CategCol("HQH2_1", "HQH2b") _,
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

  private[this] val mapYears: Map[String, String] = {
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

  private[this] val middleValuesOfRangesColumn: Map[String, Map[String, String]] = Map(
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

  private[this] val middleValuesOfRangesCategColumn4RawCol: Map[String, Map[String, String]] =
    Map(

      "HQH11b2" -> Map("0" -> "0", "1" -> "12500", "2" -> "37500", "3" -> "75000", "4" -> "125000",
                       "5" -> "175000", "6" -> "250000", "7" -> "400000", "8" -> "650000",
                       "9" -> "850000"),

      "HQH5m3" -> Map("0" -> "0", "1" -> "1.0", "2" -> "2.25", "3" -> "2.75", "4" -> "3.25",
                      "5" -> "3.75", "6" -> "4.25", "7" -> "4.75", "8" -> "5.25", "9" -> "5.75",
                      "10" -> "6.25", "11" -> "6.75", "12" -> "7.25", "13" -> "7.75",
                      "14" -> "8.25"),

      "HQH5c2" -> Map("0" -> "0", "1" -> "250", "2" -> "750", "3" -> "1250", "4" -> "1750",
                      "5" -> "2250", "6" -> "2750", "7" -> "3250", "8" -> "3750", "9" -> "4250",
                      "10" -> "4750", "11" -> "5250", "12" -> "5750", "13" -> "6250"),

      "HQH2bnew" -> Map("0" -> "0", "1" -> "2500", "2" -> "7500", "3" -> "12500", "4" -> "17500",
                        "5" -> "25000", "6" -> "40000", "7" -> "65000", "8" -> "90000",
                        "9" -> "125000", "10" -> "175000", "11" -> "350000", "12" -> "625000",
                        "13" -> "875000"),

      "HQH2b" -> Map("0" -> "0", "1" -> "25000", "2" -> "75000", "3" -> "125000", "4" -> "175000",
                     "5" -> "250000", "6" -> "400000", "7" -> "650000", "8" -> "900000",
                     "9" -> "1150000")

    )


  override def transformRow(rowNumber: Int, rowCells: Array[String]): Array[String] = {
    if (rowNumber == 0) {
      // we don't deal with the Excel header row
      rowCells
    } else {
      val newRowCells = ArrayBuffer.empty[String]
      newRowCells ++= rowCells

      for {transformation <- transformationList} {
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

  private[this] def transformColYear(columnName: String)(row: ArrayBuffer[String]): Unit = {

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

  private[this] def transformColCategorical(columnName: String)(row: ArrayBuffer[String]): Unit =
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
              "in the map 'middleValuesOfRangesColumn' must be a double")
      row(idx) = translatedMiddleVal
    }
  }

  /**
   * A curried function to infer the value of a column with a raw value in the "2014 Housing
   * Survey", from The Center for Microeconomic Data of the Federal Reserve of New York, from
   * the value of the associated (correlated) categorical column -when this second column
   * exists-, to the middle value of this second column. It clears the value of this second
   * column so as to break the relation and the K-Means not be affected by double weights.
   *
   * For example, the columns "HQH11b_1" and "HQH11b2" are correlated in the "2014 Housing
   * Survey":
   *    "HQH11b_1" has the raw value
   *    "HQH11b2" has the category (range) in which that raw value falls.
   * This method transforms the raw value (if empty) to the middle value of the range of the
   * categorical value, and then clears the categorical value. Otherwise, if the raw value
   * doesn't exist ___in all___ the Excel spreadsheet, then it converts the categorical value to
   * the middle value of its range, so that the K-Means will work instead on this transformed
   * column.
   */

  private[this] def transformRawCol4CategCol(rawColName: String, categColName: String)
      (row: ArrayBuffer[String]): Unit = {

    require(rawColName != categColName,
            "The names of the column with the raw value and the column with the associated " +
            "categorical value must be different. Ie., both columns are different.")

    require(middleValuesOfRangesCategColumn4RawCol.contains(categColName),
            s"The map 'middleValuesOfRangesCategColumn4RawCol' must have a definition " +
            s"for key '$categColName'")

    val idxCategCol = excelSpreadsh.findHeader(categColName)
    if (idxCategCol >= 0) {
      val middleValuesForCategColumn = middleValuesOfRangesCategColumn4RawCol(categColName)
      val originalCategValue = row(idxCategCol)
      val middleValueCategCol = middleValuesForCategColumn(originalCategValue)

      // the new "middleValueCategCol" corresponding to the input value "originalCategValue" must
      // be a double, for the K-Means works on double values. (To verify this condition on the map
      // "middleValueCategCol" for several rows in the input Excel spreadsheet is inefficient,
      // because it could verify the same value multiple times, for different rows in the
      // input Excel spreadsheet.)
      require(middleValueCategCol.toDouble >= Double.MinValue,
              s"The mapped value for code '$originalCategValue' in the column key " +
              s"'$categColName' in the map 'middleValuesOfRangesCategColumn4RawCol' " +
              "must be a double.")

      // the columns "rawColName" and "categColName" are linearly correlated in the "2014 Housing
      // Survey", from The Center for Microeconomic Data of the Federal Reserve of New York, so we
      // must leave only one of them in the Apache Spark RDD, otherwise in the K-Means Clustering
      // both columns will increase the weight of their distance by two, making them more important
      // than other columns in the "2014 Housing Survey", so the ML clustering is not unbiased.

      val idxRawCol = excelSpreadsh.findHeader(rawColName)
      if (idxRawCol >= 0) {
        // the raw column "rawColName" does exist in the Excel spreadsheet: see if it is empty,
        // and if so, assign to it the middle value of the related column 'categColName'
        if (row(idxRawCol) == "" || row(idxRawCol) == "0" || row(idxRawCol) == "0.0") {
          row(idxRawCol) = middleValueCategCol
        }
        // in any case, since the raw column "rawColName" exists, then clear the correlated column
        // "categColName" which also exists. This way, we are clearing the correlation and making
        // the K-Means clustering unbiased again.
        row(idxCategCol) = excelSpreadsh.fillNANullValue.toString  // this value is "0" by default.
        // An alternative to setting the value to "0" of the entry 'categColName' in the row
        // vector, would be to delete this entry 'categColName' in the vector (via a
        // "row.remove(idxCategCol)" in Scala), but then the mapping between the header names array
        // (the protected array "excelSpreadsh.header" used inside excelSpreadsh.findHeader(...))
        // and the vector data is shifted by one, because the column 'categColName' was deleted in
        // each (all) of the data vectors, but not in header names array. It is easier just to set
        // the value of 'categColName' to "0" in the data vector, just clearing the correlation
        // with the other column 'rawColName', than to wholy delete this entry 'categColName' in
        // the data vector.
      } else {
        // the column "rawColName" doesn't exist at all in the Excel spreadsheet, only the column
        // "categColName": just flatten this nominal column "categColName" into the middle value of
        // the range, so the distance calculation in the K-Means has a little more of meaning
        row(idxCategCol) = middleValueCategCol
      }
    }
  }

  private[this] def transformColHQH6a3(row: ArrayBuffer[String]): Unit = {

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


  private[this] def transformColHQ1_1(row: ArrayBuffer[String]): Unit = {

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
