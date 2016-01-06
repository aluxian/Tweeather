package com.aluxian.tweeather.models

/**
  * A model used to represent a bounding box using a pair of coordinates.
  */
case class LocationBox(sw: Coordinates, ne: Coordinates) {
  def toTwitterBox: Array[Array[Double]] = {
    Array(
      Array(sw.lon, sw.lat),
      Array(ne.lon, ne.lat)
    )
  }
}
