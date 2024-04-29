import ScalacOptions._

Global / cancelable := true

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    scalaVersion := "2.12.17",
    publish / skip := true,
    version := "UNVERSIONED"
  )
)

val sparkVersion = "3.3.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "investeringsparser",
    scalacOptions ++= scalac212Options,
    Test / parallelExecution := false,
    libraryDependencies ++= Seq(
      "io.scalaland" %% "chimney" % "0.6.2",
      "com.github.mrpowers" %% "spark-daria" % "0.39.0",
      "com.monovore" %% "decline" % "2.3.1",
      "org.typelevel" %% "cats-core" % "2.8.0",
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test,
      "com.github.dwickern" %% "scala-nameof" % "4.0.0",
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.github.mrpowers" %% "spark-daria" % "1.2.3" % Test
    )
  )
