package com.aluxian.tweeather.scripts

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait Script {

  protected lazy val scriptName = "Tweeather_" + getClass.getSimpleName.stripSuffix("$")
  protected implicit lazy val sc = {
    val conf = new SparkConf()
      .setIfMissing("spark.app.id", scriptName)
      .setIfMissing("spark.app.name", scriptName)
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    new SparkContext(conf)
  }

  protected lazy val hdfs = FileSystem.get(sc.hadoopConfiguration)
  protected lazy val sqlc = new SQLContext(sc)

  def main(args: Array[String]) {
    // Log4j properties
    Option(getClass.getResource("/com/aluxian/tweeather/res/log4j.properties")) match {
      case Some(url) => PropertyConfigurator.configure(url)
      case None => System.err.println("Unable to load log4j.properties")
    }
  }

}
