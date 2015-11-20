package com.aluxian.tweeather.scripts

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}

object TwitterStream extends Script with Logging {

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None)

    stream.foreachRDD(rdd => {
      rdd.foreach(status => {
        println(s"${status.getId} ${status.getUser.getScreenName} ${status.getText}")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
