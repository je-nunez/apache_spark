// scalastyle:off

package customNYFedBankSCE

import scala.collection.mutable.{Map => MutableMap}

import java.io.InputStream

import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFCell}
import org.apache.poi.ss.usermodel.Cell

import excel2rdd.Excel2RDD


class NYFedBankSCEExcel2RDD(val urlNYFedBankSCEExcel: InputStream)
    extends Excel2RDD(urlNYFedBankSCEExcel) {

  val headerCommentsSpreadsheet = "Codebook"

  val headerComments = MutableMap[String, String]()

  def loadHeaderComments(): Unit = {

    iterExcelRows(headerCommentsSpreadsheet,
          (row: XSSFRow) => {
            val cells = row.cellIterator    // get an iterator over the cells in this row
            var currentCol = 0
            var currentHeader = ""

            while (currentCol <= 2 && cells.hasNext)
            {
              val cell = cells.next.asInstanceOf[XSSFCell]
              currentCol = cell.getColumnIndex

              if (currentCol <= 2 && cell.getCellType == Cell.CELL_TYPE_STRING) {

                val cellValue = cell.getStringCellValue

                // all the headers for the "2014 Housing Survey", from The Center for Microeconomic
                // Data of the Federal Reserve of New York, start by the letter 'H'
                if (currentCol == 1 && cellValue.head == 'H') {
                  currentHeader = cellValue
                } else if (currentCol == 2 && currentHeader != "") {
                  headerComments += currentHeader -> cellValue
                }
              }
            }
          }
    )
    println("Just loaded the map for the description of the Excel columns:\n" + headerComments)
  }

  def getHeaderComment(headerName: String): String = {
    if (headerComments contains headerName) {
      headerComments(headerName)
    } else {
      ""
    }
  }

}

