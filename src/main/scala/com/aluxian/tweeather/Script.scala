package com.aluxian.tweeather

import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  implicit val scriptName: String = this.getClass.getSimpleName
  implicit var hdfsHostUrl: String = "hdfs://localhost:9000"

  implicit class HdfsStringInterpolator(val sc: StringContext) {
    def hdfs(args: Any*): String = hdfsHostUrl + sc.parts.mkString
  }

  def main(implicit args: Array[String]) {
    hdfsHostUrl = arg("--hdfs").getOrElse(hdfsHostUrl)

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
