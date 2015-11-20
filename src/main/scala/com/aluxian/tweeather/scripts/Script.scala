package com.aluxian.tweeather.scripts

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  implicit var scriptName: String = this.getClass.getSimpleName.stripSuffix("$")
  implicit var hdfsHostUrl: String = "hdfs://localhost:9000"

  lazy val hdfsConf = new Configuration()
  lazy val hdfs = FileSystem.get(hdfsConf)

  implicit class HdfsStringInterpolator(val sc: StringContext) {
    def hdfs(args: Any*): String = hdfsHostUrl + sc.s(args: _*)
  }

  def main(implicit args: Array[String]) {
    Option(getClass.getClassLoader.getResource("log4j.properties")) match {
      case Some(url) => PropertyConfigurator.configure(url)
      case None => System.err.println("Unable to load log4j.properties")
    }

    hdfsHostUrl = arg("hdfs").getOrElse(hdfsHostUrl)
    hdfsConf.set("fs.defaultFS", hdfsHostUrl)

    val conf = new SparkConf()
      .setAppName("Tweeather_" + scriptName)
      .setMaster(arg("master").getOrElse("local[2]"))
      .set("spark.app.id", "Tweeather")

    main(new SparkContext(conf))
  }

  def arg(name: String)(implicit args: Array[String]): Option[String] = {
    val argName = "--" + name + "="
    args.find(_.startsWith(argName)).map(_.stripPrefix(argName))
  }

  def main(sc: SparkContext)

}
