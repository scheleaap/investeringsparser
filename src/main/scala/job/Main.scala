package job

import com.github.dwickern.macros.NameOf.nameOf
import com.monovore.decline.Opts
import job.MoneyUtil.{MoneyType, parseCurrencyValue}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    println(df.schema)
  }

  private def readInputFile(spark: SparkSession)(path: String): DataFrame = {
    import spark.implicits._
    spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .option("comment", "#")
      .csv(path)
      .select(
        to_date($"Buchung", "dd.MM.yyyy").as("bookingDate"),
        to_date($"Valuta", "dd.MM.yyyy").as("valueDate"),
        col("Auftraggeber/Empf�nger").as("senderOrReceiver"), // ... or beneficiary
        $"Buchungstext".as("postingText"),
        lower($"Verwendungszweck").as("reasonForTransfer"),
        parseCurrencyValue($"Saldo").cast(MoneyType).as("balanceValue"),
        $"W�hrung6".as("balanceCurrency"),
        parseCurrencyValue($"Betrag").cast(MoneyType).as("amountValue"),
        $"W�hrung8".as("amountCurrency")
      )
    // TODO: Convert to case class
  }
}
