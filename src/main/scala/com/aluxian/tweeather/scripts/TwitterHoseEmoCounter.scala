package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

object TwitterHoseEmoCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")

    // Print count
    logInfo(s"Count = ${data.count()}")
  }

}
