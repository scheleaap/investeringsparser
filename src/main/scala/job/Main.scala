package job

import com.github.dwickern.macros.NameOf.nameOf
import com.monovore.decline.Opts
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import util.{CommandApp2, Spark}

object Main
    extends CommandApp2(
      name = nameOf(Main),
      header = "Calculates investment values of Duurzaaminvesteren.nl"
    ) {

  override def main: Opts[Unit] = {
    Opts
      .option[String]("csvPath", help = "The path to the CSV file or to the directory containing CSV files")
      .map(main(Spark.getOrCreate))
  }

  def main(spark: SparkSession)(csvPath: String): Unit = {
    val df = readInputFile(spark)(csvPath)
    df.show()
  }

  private def readInputFile(spark: SparkSession)(path: String): DataFrame = {
    import spark.implicits._
    val columnNameMapping = Map(
      "Buchung" -> "bookingDate",
      "Valuta" -> "valueDate",
      "Auftraggeber/Empf�nger" -> "senderOrReceiver", // ... or beneficiary
      "Buchungstext" -> "postingText",
      "Verwendungszweck" -> "reasonForTransfer",
      "Saldo" -> "balanceValue",
      "W�hrung6" -> "balanceCurrency",
      "Betrag" -> "amountValue",
      "W�hrung8" -> "amountCurrency"
    )
//    val format = NumberFormat.getInstance(Locale.FRANCE)
//    val number = format.parse("1,234")
    spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("comment", "#")
      .option("dateFormat", "dd.MM.yyyy")
      .csv(path)
  .select(
    $"Buchung".as("bookingDate"),
    $"Valuta".as("valueDate"),
    col("Auftraggeber/Empf�nger").as("senderOrReceiver" ), // ... or beneficiary
    $"Buchungstext".as("postingText"),
    $"Verwendungszweck".as("reasonForTransfer"),
    $"Saldo".as("balanceValue"),
    $"W�hrung6".as("balanceCurrency"),
    $"Betrag".as("amountValue"),
    $"W�hrung8".as("amountCurrency"),
    )
  }
}
