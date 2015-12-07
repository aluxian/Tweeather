package com.aluxian.tweeather.models

case class Metric(gridName: String, name: String)

object Metric {
  val Pressure = Metric("Pressure_surface", "pressure")
  val Temperature = Metric("Temperature_surface", "temperature")
  val Humidity = Metric("Relative_humidity_height_above_ground", "humidity")
}
