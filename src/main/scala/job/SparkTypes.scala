package job

import org.apache.spark.sql.types.DecimalType

object SparkTypes {
  val PercentageType: DecimalType = DecimalType(5, 2)
}
