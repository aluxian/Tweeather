package com.aluxian.tweeather.models

/**
  * A model used to represent a grid in a grib2 file.
  * @param gridName The name of the grid as it appears in the grib2 file.
  * @param name The display name of the metric.
  */
case class Metric(gridName: String, name: String)

/**
  * Common metrics used by the scripts.
  */
object Metric {
  val Pressure = Metric("Pressure_surface", "pressure")
  val Temperature = Metric("Temperature_surface", "temperature")
  val Humidity = Metric("Relative_humidity_height_above_ground", "humidity")
}
