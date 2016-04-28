// scalastyle:off

package excel2rdd

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

