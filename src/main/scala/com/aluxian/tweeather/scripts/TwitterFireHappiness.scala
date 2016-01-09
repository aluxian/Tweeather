package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row

/**
  * This script parses the tweets collected by [[TwitterFireCollector]] and exports data
  * that can be used to create a happiness graph.
  */
object TwitterFireHappiness extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    var data = sc.textFile("/tw/fire/collected/*.text")
      .coalesce(sc.defaultParallelism)
      .distinct()
      .map(_.split(','))
      .map(parts => (parts(0).toDouble, parts(1).toDouble, parts(3)))
      .toDF("lat", "lon", "raw_text")

    // Analyse sentiment
    logInfo("Analysing sentiment")
    data = PipelineModel
      .load("/tw/sentiment/models/emo.model")
      .transform(data)

    // Export data
    logInfo("Exporting data")
    data
      .select("lat", "lon", "probability")
      .map { case Row(lat, lon, probability: Vector) =>
        Seq(
          lat.toString.toDouble,
          lon.toString.toDouble,
          probability(1)
        ).mkString(",")
      }
      .saveAsTextFile("/tw/fire/parsed/happiness.txt")

    logInfo("Parsing finished")
    sc.stop()
  }

}
