import sbt._

object Dependencies {
  lazy val kcl = "com.amazonaws" % "amazon-kinesis-client" % "1.8.8"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val slf4j = "org.slf4j" % "jcl-over-slf4j" % "1.7.25"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
