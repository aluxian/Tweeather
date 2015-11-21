package com.aluxian.tweeather.scripts

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  def main(args: Array[String]) {
    // Log4j properties
    Option(getClass.getClassLoader.getResource("com/aluxian/tweeather/log4j.properties")) match {
      case Some(url) => PropertyConfigurator.configure(url)
      case None => System.err.println("Unable to load log4j.properties")
    }

    // Spark configuration
    val conf = new SparkConf()
      .set("spark.app.id", "Tweeather") // TODO: Does this have to be unique?
      .set("spark.app.name", "Tweeather_" + getClass.getSimpleName.stripSuffix("$"))
      .set("spark.master", sys.props.getOrElse("spark.master", "local[*]"))

    main(new SparkContext(conf))
  }

  def main(sc: SparkContext)

}
