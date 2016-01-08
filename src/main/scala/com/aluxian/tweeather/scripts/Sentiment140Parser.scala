package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

/**
  * This script parses the Sentiment140 dataset downloaded by [[Sentiment140Downloader]].
  * It strips redundant data and only keeps the raw text and labels.
  */
object Sentiment140Parser extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    val testData = sc.textFile("/tw/sentiment/140/downloaded/testdata.manual.2009.06.14.csv")
    val trainingData = sc.textFile("/tw/sentiment/140/downloaded/training.1600000.processed.noemoticon.csv")

    logInfo(s"Parsing test dataset")
    parse(testData, "/tw/sentiment/140/parsed/test.parquet")

    logInfo(s"Parsing training dataset")
    parse(trainingData, "/tw/sentiment/140/parsed/training.parquet")

    logInfo("Parsing finished")
    sc.stop()
  }

  def parse(data: RDD[String], filePath: String) {
    val parsed = data
      .filter(_.contains("\",\"")) // ensure correct format
      .map(_.split("\",\"").map(_.replace("\"", ""))) // split columns and remove " marks
      .filter(row => row.forall(_.nonEmpty)) // ensure columns are not empty
      .map(row => (row(0).toDouble, row(5))) // keep sentiment and text only
      .filter(row => row._1 != 2) // remove neutral tweets
      .map(row => (row._1 / 4, row._2)) // normalize sentiment
      .map(row => (row._2, row._1)) // switch values

    import sqlc.implicits._
    parsed.toDF("raw_text", "label").write.mode(SaveMode.Overwrite).parquet(filePath)
    logInfo(s"Parsed and saved $filePath")
  }

}
