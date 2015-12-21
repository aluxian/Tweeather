name := "tweeather"

version := "1.0.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT" copy (isChanging = false),
  "org.apache.spark" %% "spark-mllib" % "2.0.0-SNAPSHOT" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming" % "2.0.0-SNAPSHOT" copy (isChanging = false),
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" excludeAll ExclusionRule("javax.servlet"),
  "org.eclipse.jetty" % "jetty-servlet" % "8.1.14.v20131031",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.scalaj" %% "scalaj-http" % "2.0.0",
  "com.jsuereth" %% "scala-arm" % "1.4",
  "edu.ucar" % "grib" % "4.6.3"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
  "jline" % "jline" % "2.12.1"
)

resolvers ++= Seq(
  "Apache Snapshots" at "http://repository.apache.org/snapshots/",
  "Unidata Releases" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/"
)
