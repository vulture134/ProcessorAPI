ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "ProcessorAPI"
  )

javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.Configuration=log4j.properties")


libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
  "org.apache.kafka" % "kafka-streams" % "3.1.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.2.4",
  "org.scalatest" %% "scalatest" % "3.2.4" % "test",
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.1.0" % "test",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "org.slf4j" % "slf4j-simple" % "1.7.36",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)