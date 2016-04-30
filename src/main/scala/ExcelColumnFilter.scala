// scalastyle:off

package excel2rdd

// Note: we do a very simple column filter. A more general column filter would
// be like a
// SELECT-<expression-list>-FROM-<tables-or-views>-WHERE-<boolean-expression-list>
// in SQL. For an idea of different filters, there are pre-built filters in Weka,
// that Spark of course also supports:
//      http://weka.sourceforge.net/doc.dev/weka/filters/Filter.html (subclasses to get an idea)
// This project doesn't need either generalization.

abstract class ExcelColumnFilter {

  def filterCol(colNumber: Int, colValue: String): String

  def apply(colNumber: Int, colValue: String): String =
    filterCol(colNumber, colValue)

}

class ExcelDropColumns (val colsToDrop: Array[Int])
   extends ExcelColumnFilter {

  override def filterCol(colNumber: Int, colValue: String): String = {
    if (colsToDrop contains colNumber) "" else colValue
  }

}

