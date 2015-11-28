package com.aluxian.tweeather

import java.util.{Calendar, Date, Locale}

import com.aluxian.tweeather.models.{Coordinates, Tweet}
import twitter4j.{GeoLocation, Status}

package object utils {

  implicit class RichDate(date: Date) {
    def toCalendar: Calendar = {
      val calendar = Calendar.getInstance(Locale.US)
      calendar.setTime(date)
      calendar
    }
  }

  implicit class RichGeoLocation(location: GeoLocation) {
    def toCoordinate: Coordinates = {
      Coordinates(location.getLatitude, location.getLongitude)
    }
  }

  implicit class RichStatus(status: Status) {
    def getApproximateLocation: Coordinates = {
      if (status.getGeoLocation != null) {
        status.getGeoLocation.toCoordinate
      } else {
        val coordinates = status.getPlace.getBoundingBoxCoordinates()(0).map(_.toCoordinate)
        val sum = coordinates.reduce({ (c1, c2) => Coordinates(c1.lat + c2.lat, c1.lon + c2.lon) })
        Coordinates(sum.lat / coordinates.length, sum.lon / coordinates.length)
      }
    }

    def toTweet: Tweet = {
      Tweet(
        status.getUser.getScreenName,
        status.getText,
        status.getApproximateLocation,
        status.getCreatedAt.getTime
      )
    }
  }

  implicit class RichBoolean(boolean: Boolean) {
    def toDouble: Double = {
      if (boolean) 1d else 0d
    }
  }

  implicit class RichSeq[+A](seq: Seq[A]) {
    def mapCompose[B](z: B)(f: A => (B => B)): B = {
      seq.map(f).reduceRight(_ compose _).apply(z)
    }
  }

}
