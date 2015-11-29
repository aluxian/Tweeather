package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

object Sentiment140Parser extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    val testData = sc.textFile("/tw/sentiment/140/testdata.manual.2009.06.14.csv")
    val trainingData = sc.textFile("/tw/sentiment/140/training.1600000.processed.noemoticon.csv")

    parse(testData, "/tw/sentiment/140/test.parquet")
    parse(trainingData, "/tw/sentiment/140/training.parquet")
  }

  def parse(data: RDD[String], filePath: String) {
    logInfo(s"Parsing $filePath")

    val parsed = data
      .filter(_.contains("\",\"")) // ensure correct format
      .map(_.split("\",\"").map(_.replace("\"", ""))) // split columns and remove " marks
      .filter(row => row.forall(_.nonEmpty)) // ensure columns are not empty
      .map(row => (row(0).toDouble, row(5))) // keep sentiment and text only
      .filter(row => row._1 != 2) // remove neutral tweets
      .map(row => (row._1 / 4, row._2)) // normalize sentiment

    import sqlc.implicits._
    parsed.toDF("label", "raw_text").write.mode(SaveMode.Overwrite).save(filePath)
  }

}
