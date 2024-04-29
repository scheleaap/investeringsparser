package job

import com.github.dwickern.macros.NameOf.nameOf
import com.github.mrpowers.spark.daria.sql.DariaWriters
import com.monovore.decline.Opts
import job.MoneyUtil.{MoneyType, parseCurrencyValue}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import util.{CommandApp2, Spark}

import java.time.LocalDateTime

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

//  def main(spark: SparkSession)(csvPath: String): Unit = {
//    import spark.implicits._
//    val df = readInputFile(spark)(csvPath)
//      .transform(addAndFilterByProject(spark))
//      .groupBy($"project.name")
//      .agg(
//        sum(when($"bankStatementItem.amountValue" < 0, $"bankStatementItem.amountValue").otherwise(0)).cast(MoneyType).alias("investment"),
//        sum(when($"bankStatementItem.amountValue" >= 0, $"bankStatementItem.amountValue").otherwise(0)).cast(MoneyType).alias("repayment"),
//        sum("bankStatementItem.amountValue").cast(MoneyType).alias("balance")
//      )
//      .withColumns(
//        Map(
//          ("check", $"balance" === $"investment" + $"repayment"),
//          ("result %", ((($"repayment" / -$"investment") - 1) * 100).cast(PercentageType))
//        )
//      )
//
//    df.show()
//  }

  def main(spark: SparkSession)(csvPath: String): Unit = {
    import spark.implicits._
    val df = readInputFile(spark)(csvPath)
      .transform(addAndFilterByProject(spark))
      .select(
        $"bankStatementItem.bookingDate",
        $"bankStatementItem.amountValue".cast(MoneyType),
        $"bankStatementItem.reasonForTransfer",
        $"project.name"
      )

    val tmpFolder: String = s"/tmp/investeringsparser.${LocalDateTime.now()}"
    DariaWriters.writeSingleFile(
      df = df,
      format = "csv",
      sc = spark.sparkContext,
      tmpFolder = tmpFolder,
      filename = "out.csv"
    )
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

  private def addAndFilterByProject(spark: SparkSession)(input: Dataset[BankStatementItem]): Dataset[BankStatementItemWithProject] = {
    import spark.implicits._

    input.flatMap(i =>
      reasonForTransferToProject(i.senderOrReceiver, i.reasonForTransfer)
        .map(j => BankStatementItemWithProject(i, j))
    )
  }

  private def reasonForTransferToProject(senderOrReceiver: String, reasonForTransfer: String): Option[Project] = {
    val text1 = if (senderOrReceiver == null) "" else senderOrReceiver.replace(" ", "").toLowerCase
    val text2 = if (reasonForTransfer == null) "" else reasonForTransfer.replace(" ", "").toLowerCase

    val modifiedMapping = Projects.StringMapping.map { case (s, p) =>
      s.replace(" ", "").toLowerCase -> p
    }

    val matchedItems: Seq[(String, Project)] = modifiedMapping.flatMap({ case (s, p) =>
      if (text1.contains(s) || text2.contains(s)) {
        Seq((s, p))
      } else {
        Seq.empty
      }
    })
    val matchedProjects = matchedItems.groupBy(_._2)

    matchedProjects.size match {
      case 0 => None
      case 1 => Some(matchedProjects.head._1)
      case _ =>
        throw new RuntimeException(s"Strings '$text1' and $text2' matched multiple projects: $matchedProjects")
    }
  }
}
