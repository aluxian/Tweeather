package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{Logging, SparkContext}
import twitter4j.FilterQuery

object TwitterFireHoseCollector extends Script with Logging {

  val locationBoundingBox = Array(
    Array[Double](-27, 33),
    Array[Double](45, 73)
  ) // Europe

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Minutes(10))
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream.foreachRDD(rdd => {
      rdd.foreach(status => {
        println(s"${status.getId} ${status.getUser.getScreenName} ${status.getText}")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .locations(locationBoundingBox: _*)
      .language("en")
  }

}
