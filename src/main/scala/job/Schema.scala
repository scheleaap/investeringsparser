package job

import java.time.LocalDate

object Schema {
  object CSV {
    object ColumnNames {
      val bookingDate = "Buchung"
      val valueDate = "Valuta"
      val senderOrReceiver = "Auftraggeber/Empfänger"
      val postingText = "Buchungstext"
      val reasonForTransfer = "Verwendungszweck"
      val balanceValue = "Saldo"
      val balanceCurrency = "Währung6"
      val amountValue = "Betrag"
      val amountCurrency = "Währung8"
    }
  }

  case class BankStatementItem(
    bookingDate: LocalDate,
    valueDate: LocalDate,
    senderOrReceiver: String,
    postingText: String,
    reasonForTransfer: String,
    balanceValue: BigDecimal,
    balanceCurrency: String,
    amountValue: BigDecimal,
    amountCurrency: String
  )

  case class BankStatementItemWithProject(
    bankStatementItem: BankStatementItem,
    project: Project
  )
}
