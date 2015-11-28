package com.aluxian.tweeather.models

case class LocationBox(sw: Coordinates, ne: Coordinates) {

  def toTwitterBox: Array[Array[Double]] = {
    Array(
      Array(sw.lon, sw.lat),
      Array(ne.lon, ne.lat)
    )
  }

}
