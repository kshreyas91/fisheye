name := "fisheye"

version := "1.0"

scalaVersion := "2.12.1"

val circeVersion = "0.7.0-M2"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.22"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

// Circe

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-java8"
).map(_ % circeVersion)

// Http
libraryDependencies += "org.scalaj" % "scalaj-http_2.12" % "2.3.0"


// neo4j
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.2.1"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.2"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.10"