// scalastyle:off

package customNYFedBankSCE

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
    rowCells
  }

}

