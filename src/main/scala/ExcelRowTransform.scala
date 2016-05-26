// scalastyle:off

package excel2rdd


abstract class ExcelRowTransform {

  def transformRow(rowNumber: Int, rowCells: Array[String]): Array[String]

  def apply(rowNumber: Int, rowCells: Array[String]): Array[String] =
    transformRow(rowNumber, rowCells)

}

case object ExcelRowIdentity extends ExcelRowTransform {

  // Identity function, return original value as it is

  override def transformRow(rowNumber: Int, rowCells: Array[String]): Array[String] = rowCells

}


