package com.aluxian.tweeather.scripts.base

import org.apache.spark.{SparkConf, SparkContext}

trait SparkScript extends Script {

  val scriptName = "Tweeather_" + getClass.getSimpleName.stripSuffix("$")

  override def main(args: Array[String]) {
    super.main(args)

    // Spark configuration
    val conf = new SparkConf()
      .setIfMissing("spark.app.id", scriptName)
      .setIfMissing("spark.app.name", scriptName)
      .setIfMissing("spark.master", "local[*]")

    main(new SparkContext(conf))
  }

  def main(sc: SparkContext)

}
