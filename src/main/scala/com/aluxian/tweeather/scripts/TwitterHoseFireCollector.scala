package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.base.{Hdfs, SparkScript}
import com.aluxian.tweeather.models.{Coordinates, LocationBox}
import com.aluxian.tweeather.streaming.TwitterUtils
import com.aluxian.tweeather.utils.RichStatus
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{Logging, SparkContext}
import twitter4j.FilterQuery

object TwitterHoseFireCollector extends SparkScript with Hdfs with Logging {

  val locationBox = LocationBox(
    sw = Coordinates(33, -27),
    ne = Coordinates(73, 45)
  ) // Europe

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Minutes(10))
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .map(_.toTweet)
      .saveAsObjectFiles(hdfs"/tw/fire/data/tweet")

    ssc.start()
    ssc.awaitTermination()
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .locations(locationBox.toTwitterBox: _*)
      .language("en")
  }


}
