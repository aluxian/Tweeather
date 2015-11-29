package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.models.{Coordinates, LocationBox}
import com.aluxian.tweeather.streaming.TwitterUtils
import com.aluxian.tweeather.utils.RichStatus
import org.apache.spark.Logging
import org.apache.spark.streaming.{Minutes, StreamingContext}
import twitter4j.FilterQuery

object TwitterHoseFireCollector extends Script with Logging {

  val locationBox = LocationBox(
    sw = Coordinates(33, -27),
    ne = Coordinates(73, 45)
  ) // Europe

  override def main(args: Array[String]) {
    super.main(args)

    val ssc = new StreamingContext(sc, Minutes(10))
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .map(_.toTweet)
      .saveAsObjectFiles("/tw/fire/data/tweet")

    ssc.start()
    ssc.awaitTermination()
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .locations(locationBox.toTwitterBox: _*)
      .language("en")
  }

}
