package com.aluxian.tweeather.scripts

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row

/**
  * This script parses the tweets collected by [[TwitterFireCollector]] and exports
  * a csv file that can be used to plot a happiness graph.
  */
object TwitterFireHappiness extends Script with Logging {

  val happinessTextPath = new Path("/tw/fire/parsed/happiness.text")
  val happinessCsvPath = new Path("/tw/fire/parsed/happiness.csv")

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

    // Remove existing files
    logInfo("Removing existing files")
    hdfs.delete(happinessTextPath, true)
    hdfs.delete(happinessCsvPath, true)

    // Export data
    logInfo("Exporting data")
    data
      .select("lat", "lon", "probability")
      .map { case Row(lat, lon, probability: Vector) =>
        Seq(
          lat.toString,
          lon.toString,
          probability(1)
        ).mkString(",")
      }
      .saveAsTextFile("/tw/fire/parsed/happiness.text")

    // Merge files into a single csv
    logInfo("Merging csv")
    FileUtil.copyMerge(hdfs, happinessTextPath, hdfs, happinessCsvPath, true, hdfs.getConf, null)

    logInfo("Parsing finished")
    sc.stop()
  }

}
