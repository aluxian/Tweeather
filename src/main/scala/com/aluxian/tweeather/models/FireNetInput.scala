package com.aluxian.tweeather.models

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

case class FireNetInput(sentiment: Double,
                        temperature: Double,
                        pressure: Double,
                        humidity: Double) {

  def toLabeledPoint: LabeledPoint = {
    new LabeledPoint(sentiment, Vectors.dense(temperature, pressure, humidity))
  }

}
