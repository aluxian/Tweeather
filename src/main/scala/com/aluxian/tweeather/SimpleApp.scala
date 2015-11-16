package com.aluxian.tweeather

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Tweeather")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
  }

}
