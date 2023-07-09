package job

import com.github.dwickern.macros.NameOf.nameOf
import com.monovore.decline.Opts
import job.MoneyUtil.{parseCurrencyValue, MoneyType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
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
    import spark.implicits._
    val df = readInputFile(spark)(csvPath)
      .transform(addAndFilterByProject(spark))
      .groupBy($"project.name")
      .agg(sum("bankStatementItem.amountValue"))
    df.show()
  }

  private def readInputFile(spark: SparkSession)(path: String): Dataset[BankStatementItem] = {
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
      .as[BankStatementItem]
  }

  private def addAndFilterByProject(spark: SparkSession)(input: Dataset[BankStatementItem]): Dataset[(BankStatementItemWithProject)] = {
    import spark.implicits._

    input.flatMap(i =>
      reasonForTransferToProject(i.senderOrReceiver, i.reasonForTransfer)
        .map(j => BankStatementItemWithProject(i, j))
    )
  }

  private def reasonForTransferToProject(senderOrReceiver: String, reasonForTransfer: String): Option[Project] = {
    if (reasonForTransfer == null) {
      None
    } else if (reasonForTransfer.contains("29213")) {
      Some(Projects.WindparkDenTol)
    } else if (reasonForTransfer.contains("45903")) {
      Some(Projects.DeGroeneAggregaat)
    } else if (senderOrReceiver.contains("Groene Aggregaat")) {
      Some(Projects.DeGroeneAggregaat)
    } else {
      None
    }
    // TODO:
    // Grienr
    // Trading Energy Solutions in Sust BV
  }
}
