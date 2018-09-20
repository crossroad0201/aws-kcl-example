import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "kclexample",
      scalaVersion := "2.12.5",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "scala-example",

    libraryDependencies ++= Seq(
      kcl,
      logback,
      slf4j,
      scalaTest % Test
    )
  )
