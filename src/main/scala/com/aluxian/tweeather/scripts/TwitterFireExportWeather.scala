package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichSeq
import com.aluxian.tweeather.scripts.TwitterFireParser._
import com.aluxian.tweeather.transformers.{GribUrlGenerator, WeatherProvider}
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.Row

/**
  * This script parses the tweets collected by [[TwitterFireCollector]] and exports
  * a csv file with the weather conditions for each tweet's location.
  */
object TwitterFireExportWeather extends Script with Logging {

  val weatherTextPath = new Path("/tw/fire/parsed/weather.text")
  val weatherCsvPath = new Path("/tw/fire/parsed/weather.csv")

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    var data = sc.textFile("/tw/fire/collected/*.text")
      .coalesce(sc.defaultParallelism)
      .distinct()
      .map(_.split(','))
      .map(parts => (parts(0).toDouble, parts(1).toDouble, parts(2).toLong))
      .toDF("lat", "lon", "createdAt")

    // Get weather
    logInfo("Getting weather data")
    data = Seq(
      new GribUrlGenerator().setLocationBox(locationBox).setInputCol("createdAt").setOutputCol("grib_url"),
      new WeatherProvider().setGribUrlColumn("grib_url")
    ).mapCompose(data)(_.transform)

    // Restore number of partitions
    logInfo(s"Restoring number of partitions to ${sc.defaultParallelism}")
    data = data.repartition(sc.defaultParallelism)

    // Remove existing files
    logInfo("Removing existing files")
    hdfs.delete(weatherTextPath, true)
    hdfs.delete(weatherCsvPath, true)

    // Export data
    logInfo("Exporting data")
    data
      .select("lat", "lon", "createdAt", "temperature", "pressure", "humidity")
      .map { case Row(lat, lon, createdAt, temperature, pressure, humidity) =>
        Seq(
          lat.toString,
          lon.toString,
          createdAt.toString,
          temperature.toString,
          pressure.toString,
          humidity.toString
        ).mkString(",")
      }
      .saveAsTextFile("/tw/fire/parsed/weather.text")

    // Merge files into a single csv
    logInfo("Merging csv")
    FileUtil.copyMerge(hdfs, weatherTextPath, hdfs, weatherCsvPath, true, hdfs.getConf, null)

    logInfo("Parsing finished")
    sc.stop()
  }

}
