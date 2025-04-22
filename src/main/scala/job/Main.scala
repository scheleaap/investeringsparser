package job

import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.dwickern.macros.NameOf.nameOf
import com.github.mrpowers.spark.daria.sql.DariaWriters
import com.monovore.decline.Opts
import job.MoneyUtil.{parseCurrencyValue, MoneyType}
import job.Schema.{BankStatementItem, BankStatementItemWithProject, CSV}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import util.{CommandApp2, Spark}

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import scala.jdk.CollectionConverters._

object Main
    extends CommandApp2(
      name = nameOf(Main),
      header = "Calculates investment values of Duurzaaminvesteren.nl"
    ) {

  override def main: Opts[Unit] = {
    (
      Opts
        .option[Path]("inputFile", help = "The path to the input CSV file"),
      Opts.option[Path]("outputFile", help = "The path to the output CSV file").orNone
    )
      .mapN(main(Spark.getOrCreate))
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

  def main(spark: SparkSession)(inputFile: Path, outputFileOpt: Option[Path]): Unit = {
    import spark.implicits._
    val df = readInputFile(spark)(
      inputFile = inputFile,
      charset = StandardCharsets.ISO_8859_1,
      linesToSkip = 13
    )
      .transform(addAndFilterByProject(spark))
      .select(
        $"bankStatementItem.bookingDate",
        $"bankStatementItem.amountValue".cast(MoneyType),
        $"bankStatementItem.reasonForTransfer",
        $"project.name"
      )

    val outputFile = outputFileOpt.getOrElse(Path.of("out.csv"))
    val tmpFolder: String = s"/tmp/investeringsparser.${LocalDateTime.now()}"
    DariaWriters.writeSingleFile(
      df = df,
      format = "csv",
      sc = spark.sparkContext,
      tmpFolder = tmpFolder,
      filename = outputFile.toString
    )
  }

  private def readInputFile(spark: SparkSession)(inputFile: Path, charset: Charset, linesToSkip: Int): Dataset[BankStatementItem] = {
    import spark.implicits._

    // The disadvantage of this is that the application does not support patterns like "*.csv" anymore.
    val contentWithoutPrefix = Files
      .readString(inputFile, charset)
      .lines()
      .skip(linesToSkip)
      .toList
      .asScala

    spark.read
      .format("csv")
      .options(
        Map(
          "delimiter" -> ";",
          "quote" -> "\"",
          "escape" -> "\"",
          "header" -> "true",
          "inferSchema" -> "false",
          "mode" -> "FAILFAST",
          "unescapedQuoteHandling" -> "RAISE_ERROR",
          "lineSep" -> "\n"
        )
      )
      .csv(contentWithoutPrefix.toDS)
      .select(
        to_date(col(CSV.ColumnNames.bookingDate), "dd.MM.yyyy").as(nameOf[BankStatementItem](_.bookingDate)),
        to_date(col(CSV.ColumnNames.valueDate), "dd.MM.yyyy").as(nameOf[BankStatementItem](_.valueDate)),
        col(CSV.ColumnNames.senderOrReceiver).as(nameOf[BankStatementItem](_.senderOrReceiver)), // ... or beneficiary
        col(CSV.ColumnNames.postingText).as(nameOf[BankStatementItem](_.postingText)),
        lower(col(CSV.ColumnNames.reasonForTransfer)).as(nameOf[BankStatementItem](_.reasonForTransfer)),
        parseCurrencyValue(col(CSV.ColumnNames.balanceValue)).cast(MoneyType).as(nameOf[BankStatementItem](_.balanceValue)),
        col(CSV.ColumnNames.balanceCurrency).as(nameOf[BankStatementItem](_.balanceCurrency)),
        parseCurrencyValue(col(CSV.ColumnNames.amountValue)).cast(MoneyType).as(nameOf[BankStatementItem](_.amountValue)),
        col(CSV.ColumnNames.amountCurrency).as(nameOf[BankStatementItem](_.amountCurrency))
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
