package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.scripts.TwitterHoseEmoParser._
import org.apache.spark.Logging

import scala.io.StdIn

object TwitterHoseEmoCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")

    // Print count
    logInfo(s"Count = ${data.count()}")

    // Pause to keep the web UI running
    logInfo("Press enter to continue")
    StdIn.readLine()
  }

}
