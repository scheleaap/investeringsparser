import ScalacOptions.*

Global / cancelable := true

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    scalaVersion := "2.12.20",
    publish / skip := true,
    version := "UNVERSIONED"
  )
)

val sparkVersion = "3.5.5"

lazy val root = project
  .in(file("."))
  .settings(
    name := "investeringsparser",
    scalacOptions ++= scalac212Options,
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / parallelExecution := false,
    libraryDependencies ++= Seq(
      "io.scalaland" %% "chimney" % "0.6.2",
      "com.github.mrpowers" %% "spark-daria" % "0.39.0",
      "com.monovore" %% "decline" % "2.3.1",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test,
      "com.github.dwickern" %% "scala-nameof" % "5.0.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "com.github.mrpowers" %% "spark-daria" % "1.2.3" % Test
    )
  )
