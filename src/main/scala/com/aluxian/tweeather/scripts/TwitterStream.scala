package com.aluxian.tweeather.scripts

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}
import twitter4j.TwitterFactory
import twitter4j.auth.{AccessToken, Authorization}

object TwitterStream extends Script with Logging {

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, twitterAuth())
//    stream.getReceiver().
  }

  def twitterAuth(): Option[Authorization] = {
    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(arg("--twitterConsumerKey").get, arg("--twitterConsumerSecret").get)
    twitter.setOAuthAccessToken(new AccessToken(arg("--twitterToken").get, arg("--twitterTokenSecret").get))
    Option(twitter.getAuthorization)
  }

}
