package com.aluxian.tweeather.models

case class Metric(gridName: String, name: String)

object Metric {
  val Pressure = Metric("Pressure_surface", "temperature")
  val Temperature = Metric("Temperature_surface", "pressure")
  val Humidity = Metric("Relative_humidity_height_above_ground", "humidity")
}
