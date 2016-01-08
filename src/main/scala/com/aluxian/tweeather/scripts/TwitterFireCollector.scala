package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichStatus
import com.aluxian.tweeather.models.{Coordinates, LocationBox}
import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import twitter4j.FilterQuery

/**
  * This script uses Twitter Streaming API to collect tweets which are localised in Europe
  * and are written in English. It uses one or more Twitter apps, whose credentials
  * are stored '''com/aluxian/tweeather/res/twitter.properties'''.
  */
object TwitterFireCollector extends Script with Logging {

  val locationBox = LocationBox(
    sw = Coordinates(33, -27),
    ne = Coordinates(73, 45)
  ) // Europe

  override def main(args: Array[String]) {
    super.main(args)

    val ssc = new StreamingContext(sc, streamingInterval)
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .map(status => {
        val location = status.getApproximateLocation
        val text = status.getText.replaceAll("[\\n\\r,]+", " ")
        Seq(location.lat, location.lon, status.getCreatedAt.getTime, text).mkString(",")
      })
      .repartition(sc.defaultParallelism)
      .saveAsTextFiles("/tw/fire/collected/", "text")

    ssc.start()

    if (!ssc.awaitTerminationOrTimeout(streamingTimeout)) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .locations(locationBox.toTwitterBox: _*)
      .language("en")
  }

}
