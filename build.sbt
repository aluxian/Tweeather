import sbtassembly.AssemblyPlugin.autoImport._
import sbtsparksubmit.SparkSubmitPlugin.autoImport._

name := "tweeather"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.scalaj" %% "scalaj-http" % "2.0.0",
  "com.jsuereth" %% "scala-arm" % "1.4",
  "com.jcabi" % "jcabi-log" % "0.17",
  "edu.ucar" % "grib" % "4.6.3"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.4",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value
)

resolvers ++= Seq(
  "Unidata Releases" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/"
)

// Use assembly to create the uber jar
sparkSubmitJar := assembly.value.getAbsolutePath

// Import submit tasks
SparkSubmit.configurations
