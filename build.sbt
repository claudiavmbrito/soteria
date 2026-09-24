name := "SOTERIA"

version := "2.0.0-SNAPSHOT"

// Spark 3.5.x is published for Scala 2.12 and 2.13; 2.12.18 matches the Spark 3.5 build.
scalaVersion := "2.12.18"

val sparkVersion = "3.5.9"

libraryDependencies ++= Seq(
  // Spark is supplied by the cluster (or by the Gramine enclave image) at runtime.
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

// `sbt run` should see the Provided Spark jars.
Compile / run := Defaults
  .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
  .evaluated

// Assembly plugin settings for creating fat JARs
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
  case PathList("META-INF", _ @_*)             => MergeStrategy.discard
  case "application.conf"                      => MergeStrategy.concat
  case "reference.conf"                        => MergeStrategy.concat
  case _                                       => MergeStrategy.first
}

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-release", "17"
)

javacOptions ++= Seq("--release", "17")

// Spark needs these module openings on Java 17+.
val sparkJavaOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)

run / fork := true
run / javaOptions ++= sparkJavaOpts :+ "-Dspark.master=local[*]"

Test / fork := true
Test / javaOptions ++= sparkJavaOpts ++ Seq("-Xmx2g")
Test / parallelExecution := false
