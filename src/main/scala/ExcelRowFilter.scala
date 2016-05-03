// scalastyle:off

package excel2rdd

// Note: we do a very simple row filter to know how to handle the header
// row in the Excel spreadsheet, and this is why we pass as parameter the
// rowNumber. A more general row filter, which we don't need, would receive
// two boolean functions, one for knowing when to execute the saveFunc(),
// and another to know when to execute extractFunc(). In the most general
// case, the row filter is like a match-case statement,
// Array[XSSFRow => Boolean] for the guard cases, and another array
// Array[XSSFRow => Unit] for the functions to be executed when their
// corresponding guard case matches. For an idea of different filters, there
// are pre-built filters in Weka, that Spark of course also supports:
//      http://weka.sourceforge.net/doc.dev/weka/filters/Filter.html (subclasses to get an idea)
// This project doesn't need this amplitude.


abstract class ExcelRowFilter {

  def filterRow(rowNumber: Int, rowValue: String,
                saveFunc: String => Unit,
                extractFunc: String => Unit): Unit

  def apply(rowNumber: Int, rowValue: String,
            saveFunc: String => Unit,
            extractFunc: String => Unit): Unit =
    filterRow(rowNumber, rowValue, saveFunc, extractFunc)

}

case object ExcelNoHeader extends ExcelRowFilter {

  override def filterRow(rowNumber: Int, rowValue: String,
                         saveFunc: String => Unit,
                         extractFunc: String => Unit): Unit = {
    saveFunc(rowValue)
  }

}


case object ExcelHeaderDiscard extends ExcelRowFilter {

  override def filterRow(rowNumber: Int, rowValue: String,
                         saveFunc: String => Unit,
                         extractFunc: String => Unit): Unit = {
    if (rowNumber != 0) {
      saveFunc(rowValue)
    }
  }

}


case object ExcelHeaderExtract extends ExcelRowFilter {

  override def filterRow(rowNumber: Int, rowValue: String,
                         saveFunc: String => Unit,
                         extractFunc: String => Unit): Unit = {
    if (rowNumber != 0) {
      saveFunc(rowValue)
    } else {
      extractFunc(rowValue)
    }
  }

}

