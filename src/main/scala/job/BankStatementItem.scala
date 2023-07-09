package job

import java.time.LocalDate

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
