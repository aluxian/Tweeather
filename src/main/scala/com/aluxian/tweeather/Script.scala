package com.aluxian.tweeather

import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  def main(implicit args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Tweeather_" + this.getClass.getSimpleName)
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
