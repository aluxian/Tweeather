name := "spark"

version := "1.0.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-streaming" % "1.5.2",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2"
)
