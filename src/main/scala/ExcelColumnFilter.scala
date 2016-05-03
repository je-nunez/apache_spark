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

  def filterCol(colNumber: Int, colValue: String): Option[String]

  def apply(colNumber: Int, colValue: String): Option[String] =
    filterCol(colNumber, colValue)

}


case object ExcelColumnIdentity extends ExcelColumnFilter {

  // Identity function, return original value as it is

  override def filterCol(colNumber: Int, colValue: String): Option[String] = Some(colValue)

}


class ExcelDropColumns (val colsToDrop: Array[Int])
    extends ExcelColumnFilter {

  override def filterCol(colNumber: Int, colValue: String): Option[String] = {
    if (colsToDrop contains colNumber) None else Some(colValue)
  }

}

