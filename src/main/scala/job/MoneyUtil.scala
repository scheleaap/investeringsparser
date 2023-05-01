package job

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DecimalType

import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale

object MoneyUtil {

  val MoneyType: DecimalType = DecimalType(9, 2)

  val parseCurrencyValue: UserDefinedFunction = {
    val format = NumberFormat.getNumberInstance(Locale.GERMANY)
    format match {
      case i: DecimalFormat => i.setParseBigDecimal(true)
      case _ =>
    }

    udf((s: String) => {
      val number = format.parse(s)
      number match {
        case i: java.math.BigDecimal => i
        case i => throw new RuntimeException(s"Unexpected value $i")
      }
    })
  }
}
