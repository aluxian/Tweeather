package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

import scala.io.StdIn

/**
  * This script is used to count the number of rows that [[TwitterHoseFireCollector]] has collected.
  */
object TwitterHoseFireCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/fire/collected/*.text")

    // Print count
    logInfo(s"Count = ${data.count()}")

    // Pause to keep the web UI running
    logInfo("Press enter to continue")
    StdIn.readLine()
  }

}
