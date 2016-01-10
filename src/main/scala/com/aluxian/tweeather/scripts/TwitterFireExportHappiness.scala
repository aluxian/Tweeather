package com.aluxian.tweeather.scripts

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.aluxian.tweeather.RichDate
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row

/**
  * This script parses the tweets collected by [[TwitterFireCollector]] and exports
  * a csv file with the tweets and their location, timestamp, and polarity.
  */
object TwitterFireExportHappiness extends Script with Logging {

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
      .map(parts => (parts(0), parts(1), parts(2), parts(3)))
      .toDF("lat", "lon", "timestamp", "raw_text")

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
      .select("lat", "lon", "timestamp", "probability")
      .map { case Row(lat, lon, timestamp, probability: Vector) =>
        val dateFormatter = new SimpleDateFormat("yyyyMMdd", Locale.US)
        val date = new Date(timestamp.toString.toLong)
        val dateStr = dateFormatter.format(date)
        val cycle = date.toCalendar.get(Calendar.HOUR_OF_DAY) match {
          case h if h < 6 => "00"
          case h if h < 12 => "06"
          case h if h < 18 => "12"
          case _ => "18"
        }

        Seq(
          lat.toString,
          lon.toString,
          dateStr + cycle,
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
