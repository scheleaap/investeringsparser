package job

import com.github.dwickern.macros.NameOf.nameOf

case class Project(name: String)

object Projects {
  val DeGroeneAggregaat: Project = Project(nameOf(DeGroeneAggregaat))
  val WindparkDenTol: Project = Project(nameOf(WindparkDenTol))
}

