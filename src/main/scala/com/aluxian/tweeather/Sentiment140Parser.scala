package com.aluxian.tweeather

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Sentiment140Parser {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    val testData = sc.textFile("hdfs://tw/sentiment140/test.csv", 2)
    val trainingData = sc.textFile("hdfs://tw/sentiment140/training.csv", 8)

    parse(sqlContext, testData, "hdfs://tw/parsed/test.parquet")
    parse(sqlContext, trainingData, "hdfs://tw/parsed/training.parquet")
  }

  def parse(sqlContext: SQLContext, data: RDD[String], filePath: String) {
    var parsed = data
      .filter(_.contains("\",\"")) // ensure correct format
      .map(_.split("\",\"").map(_.replace("\"", ""))) // split columns and remove " marks
      .filter(row => row.forall(_.nonEmpty)) // ensure columns are not empty
      .map(row => (row(0).toInt, row(5))) // keep sentiment and text only
      .filter(row => row._1 != 2) // remove neutral tweets
      .map(row => (row._1 / 4, row._2)) // normalize sentiment

    // Sanitize text
    parsed = parsed.map(row => (row._1, row._2
      .replaceAll("&amp;|&gt;|&lt;|&quot;|&#39;", "") // html entities
      .replaceAll("[\\uE000-\\uF8FF]", "") // unicode symbols
      .replaceAll("https?:\\/\\/[^\\s]*", "") // urls
      .replaceAll("\\bvia\\b|\\bRT\\b", "") // 'via' and 'RT' keywords
      .replaceAll("\\s+", " ") // multiple white spaces
      .replaceAll("\\B@\\w*", "") // @ mentions
      .trim)
    )

    import sqlContext.implicits._
    parsed.toDF("sentiment", "text").write.save(filePath)
  }

}
