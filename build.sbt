name := "thundercats"
organization := "tao"
licenses := Seq("Apache 2.0 License" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

val sparkVersion      = "2.4.4"
val scalatest         = "3.0.3"
val framelessVersion  = "0.8.0" // for Spark 2.4.0
val scalaLogVersion   = "3.9.2"
val logbackVersion    = "1.1.2"
val circeVersion      = "0.11.1"
val kafkaVersion      = "2.1.0"
val avro4sVersion     = "3.0.4"
scalaVersion         := "2.12.0"

// Compatibility NOTE: 
// https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html#jdk-12-compatibility-notes

// Circe
libraryDependencies ++= List(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

// Following dependency is for typesafe Avro schema generator
// Unfortunately it's Jackson4s is not compatible with Spark 2.4
// ------------------------------
// val miscDependencies = List(
//   "com.sksamuel.avro4s" %% "avro4s-core" % avro4sVersion excludeAll(
//     ExclusionRule("org.scala-lang.modules",s"scala-xml_${scalaVersion.toString.dropRight(2)}"),
//     ExclusionRule("org.apache.avro", "avro"))
// )

// Test
val devDependencies = List(
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

// Spark
val sparkDependencies = List(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"
)

lazy val thundercats = project
  .settings(name := "thundercats")
  .settings(libraryDependencies ++= sparkDependencies ++ devDependencies)
