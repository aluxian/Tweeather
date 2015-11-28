package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.base.{Hdfs, SparkScript}
import com.aluxian.tweeather.models.{FireNetInput, Tweet}
import com.aluxian.tweeather.transformers._
import com.aluxian.tweeather.utils.{RichSeq, SentimentModels}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

object TwitterHoseFireParser extends SparkScript with Hdfs with Logging {

  val locationBox = TwitterHoseFireCollector.locationBox // Europe

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Import data
    var data = sc.objectFile[Tweet](hdfs"/tw/fire/data/tweet*")
      .map(tweet => (tweet.text, tweet.location.lat, tweet.location.lon, tweet.createdAt))
      .toDF("raw_text", "lat", "lon", "createdAt")

    // Analyse sentiment
    data = SentimentModels.emoModel()
      .setPredictionCol("sentiment")
      .transform(data)

    // Get weather
    data = Seq(
      new GribUrlGenerator().setLocationBox(locationBox).setInputCol("createdAt").setOutputCol("grib_url"),
      new WeatherProvider().setLatitudeColumn("grib_url")
    ).mapCompose(data)(_.transform)

    // Convert to LabeledPoint
    val libsvmData = data
      .select("sentiment", "temperature", "pressure", "humidity")
      .map({ case Row(sentiment, temperature, pressure, humidity) =>
        FireNetInput(
          sentiment.toString.toDouble,
          temperature.toString.toDouble,
          pressure.toString.toDouble,
          humidity.toString.toDouble
        ).toLabeledPoint
      })

    // Save in LIBSVM format
    MLUtils.saveAsLibSVMFile(libsvmData, hdfs"/tw/fire/data.libsvm")
  }

}
