name := "thundercats"
organization := "tao"
licenses := Seq("Apache 2.0 License" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

val sparkVersion      = "3.2.1"
val scalatest         = "3.0.3"
val framelessVersion  = "0.8.0" // for Spark 2.4.0
val scalaLogVersion   = "3.9.2"
val logbackVersion    = "1.1.2"
val circeVersion      = "0.11.1"
val kafkaVersion      = "2.1.0"
val avro4sVersion     = "3.0.4"
val emrVersion        = "4.16.0"
scalaVersion         := "2.13.1"
sbtVersion           := "1.3.3"

javacOptions in Compile ++= Seq("-source", "1.17", "-target", "1.17", "-Xlint:unchecked", "-Xlint:deprecation")

// Compatibility NOTE: 
// https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html#jdk-12-compatibility-notes

// Amazon DynamoDB
val dynamoDependencies = List(
  // "com.amazon.emr" % "emr-dynamodb-hive",
  "com.amazon.emr" % "emr-dynamodb-connector",
  "com.amazon.emr" % "emr-dynamodb-hadoop"
).map(_ % emrVersion)

resolvers += Resolver.mavenLocal

// Logging
libraryDependencies ++= List(
  "org.apache.logging.log4j" % "log4j-api",
  "org.apache.logging.log4j" % "log4j-core"
).map(_ % "2.13.3")

// Test
val devDependencies = List(
  "org.scalatest" %% "scalatest" % "3.2.11" % "test"
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
  .settings(libraryDependencies ++= sparkDependencies ++ dynamoDependencies ++ devDependencies)

lazy val samples = project
  .settings(name := "samples")
  .settings(libraryDependencies ++= sparkDependencies ++ devDependencies)
  .dependsOn(thundercats)

  import ReleaseTransformations._

releaseVersionBump := sbtrelease.Version.Bump.Next
releaseVersionFile := baseDirectory.value / "version.sbt"

publishConfiguration := publishConfiguration.value.withOverwrite(true)
releaseIgnoreUntrackedFiles := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  // runClean,                               // : ReleaseStep
  // runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  // publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  // releaseStepTask(publish in Docker),     // : ReleaseStep, publish the docker image in your specified repository(e.i. Nexus)
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)
