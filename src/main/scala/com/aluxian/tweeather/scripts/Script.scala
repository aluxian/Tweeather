package com.aluxian.tweeather.scripts

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  implicit var scriptName: String = this.getClass.getSimpleName.stripSuffix("$")
  implicit var hdfsHostUrl: String = "hdfs://localhost:9000"

  lazy val hdfsConf = new Configuration()
  lazy val hdfs = FileSystem.get(hdfsConf)

  implicit class HdfsStringInterpolator(val sc: StringContext) {
    def hdfs(args: Any*): String = hdfsHostUrl + sc.parts.mkString
  }

  def main(implicit args: Array[String]) {
    hdfsHostUrl = arg("--hdfs").getOrElse(hdfsHostUrl)
    hdfsConf.set("fs.default.name", hdfsHostUrl)

    val conf = new SparkConf()
      .setAppName("Tweeather_" + scriptName)
      .setMaster(arg("--master").getOrElse("local[2]"))

    main(new SparkContext(conf))
  }

  def arg(name: String)(implicit args: Array[String]): Option[String] = {
    val pi = args.indexOf(name)

    if (args.length > pi + 1) {
      return Option(args(pi + 1))
    }

    Option.empty
  }

  def main(sc: SparkContext)

}
