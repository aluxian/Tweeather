name := "spark"

version := "1.0.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-streaming" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-mllib" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)
