name := "tweeather"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "org.apache.spark" %% "spark-streaming" % "1.5.2",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "org.scalaj" %% "scalaj-http" % "2.0.0",
  "edu.ucar" % "grib" % "4.6.3"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

resolvers ++= Seq(
  "Unidata Releases" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/"
)
