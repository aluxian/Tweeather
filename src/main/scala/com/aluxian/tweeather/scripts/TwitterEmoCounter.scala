package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

/**
  * This script is used to count the number of rows that [[TwitterEmoCollector]] has collected.
  */
object TwitterEmoCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")

    // Print count
    logInfo(s"Count = ${data.count()}")
    sc.stop()
  }

}
