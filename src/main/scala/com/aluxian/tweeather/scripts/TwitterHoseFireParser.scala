package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichSeq
import com.aluxian.tweeather.transformers._
import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row

object TwitterHoseFireParser extends Script with Logging {

  val locationBox = TwitterHoseFireCollector.locationBox // Europe

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    var data = sc.textFile("/tw/fire/collected/*.text")
      .map(_.split(','))
      .map(parts => (parts(0).toDouble, parts(1).toDouble, parts(2).toLong, parts.drop(3).mkString(",")))
      .toDF("lat", "lon", "createdAt", "raw_text")

    // Analyse sentiment
    logInfo("Analysing sentiment")
    data = PipelineModel.load("/tw/sentiment/models/emo.model")
      .asInstanceOf[NaiveBayesModel]
      .setPredictionCol("sentiment")
      .transform(data)

    // Get weather
    logInfo("Getting weather data")
    data = Seq(
      new GribUrlGenerator().setLocationBox(locationBox).setInputCol("createdAt").setOutputCol("grib_url"),
      new WeatherProvider().setLatitudeColumn("grib_url").setGribsPath("/tw/fire/gribs/")
    ).mapCompose(data)(_.transform)

    // Convert to LabeledPoint
    logInfo("Converting to LabeledPoint format")
    val libsvmData = data
      .select("sentiment", "temperature", "pressure", "humidity")
      .map({ case Row(sentiment: Double, temperature: Double, pressure: Double, humidity: Double) =>
        new LabeledPoint(sentiment, Vectors.dense(temperature, pressure, humidity))
      })

    // Save in LIBSVM format
    logInfo("Saving data in LIBSVM format")
    MLUtils.saveAsLibSVMFile(libsvmData, "/tw/fire/parsed/data.libsvm")
  }

}
