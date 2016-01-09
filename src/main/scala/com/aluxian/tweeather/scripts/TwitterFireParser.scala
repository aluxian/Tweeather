package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichSeq
import com.aluxian.tweeather.transformers._
import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SaveMode}

/**
  * This script parses the tweets collected by [[TwitterFireCollector]].
  * For each tweet, it analyses the sentiment and retrives the weather forecast for its location.
  * The resulting dataset is coalesced to reduce the number of partitions.
  */
object TwitterFireParser extends Script with Logging {

  val locationBox = TwitterFireCollector.locationBox // Europe

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    var data = sc.textFile("/tw/fire/collected/*.text")
      .coalesce(sc.defaultParallelism)
      .distinct()
      .map(_.split(','))
      .map(parts => (parts(0).toDouble, parts(1).toDouble, parts(2).toLong, parts(3)))
      .toDF("lat", "lon", "createdAt", "raw_text")

    // Analyse sentiment
    logInfo("Analysing sentiment")
    data = PipelineModel
      .load("/tw/sentiment/models/emo.model")
      .transform(data)
      .drop("rawPrediction")
      .drop("prediction")

    // Get weather
    logInfo("Getting weather data")
    data = Seq(
      new GribUrlGenerator().setLocationBox(locationBox).setInputCol("createdAt").setOutputCol("grib_url"),
      new WeatherProvider().setGribUrlColumn("grib_url")
    ).mapCompose(data)(_.transform)

    // Restore number of partitions
    logInfo(s"Restoring number of partitions to ${sc.defaultParallelism}")
    data = data.repartition(sc.defaultParallelism)

    // Export data
    logInfo("Exporting data")
    data
      .select("probability", "temperature", "pressure", "humidity")
      .map { case Row(probability: Vector, temperature, pressure, humidity) =>
        Seq(
          probability(1),
          temperature.toString.toDouble,
          pressure.toString.toDouble,
          humidity.toString.toDouble
        ).mkString(",")
      }
      .toDF.write.mode(SaveMode.Overwrite).parquet("/tw/fire/parsed/data.parquet")

    logInfo("Parsing finished")
    sc.stop()
  }

}
