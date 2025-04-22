package job

case class Project(name: String)

object Projects {
  val DeGroeneAggregaat: Project = Project("2021 De Groene Aggregaat")
  val Grienr: Project = Project("2021 GRIENR")
  val SamenVoorGrond2: Project = Project("2021 Samen voor Grond II")
  val Volgroen1: Project = Project("2021 Volgroen MKB Energie Financiering")
  val WindparkDenTol: Project = Project("2021 Windpark Den Tol")
  val WocozonTesis: Project = Project("2021 Wocozon TESIS")
  val Opcharge2: Project = Project("2022 Opcharge SPV2 B.V. (Tranche 2)")
  val Volgroen2: Project = Project("2022 Volgroen MKB Energie Financiering (Ronde 2)")
  val MisterGreen13: Project = Project("2023 MisterGreen (Serie 13)")

  val StringMapping: Seq[(String, Project)] = Seq(
    ("45903", DeGroeneAggregaat),
    ("Groene Aggregaat", DeGroeneAggregaat),
    ("30344", Grienr),
    ("grienr", Grienr),
    ("45904", SamenVoorGrond2),
    ("Samen voor Grond II", SamenVoorGrond2),
    ("39042", Volgroen1),
    ("Volgroen MKB Energie Financiering", Volgroen1),
    ("29213", WindparkDenTol),
    ("Windpark Den Tol", WindparkDenTol),
    ("30119", WocozonTesis),
    ("Wocozon TESIS", WocozonTesis),
    ("54796", Opcharge2),
    ("OPCHARGE SPV2", Opcharge2),
    ("53926", Volgroen2),
    ("Volgroen Stroom 2", Volgroen2),
    ("66864", MisterGreen13),
    ("13 Mist erGreen", MisterGreen13)
  )
}
