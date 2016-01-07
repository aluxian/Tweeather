package com.aluxian.tweeather.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{FilterQuery, Status}

/**
  * A stream of Twitter statuses, potentially filtered by a query. The Twitter API is such that this may return a
  * sampled subset of all tweets during each interval.
  *
  * If no Authorization object is provided, initializes OAuth authorization using the system
  * properties twitter4j.oauth.{consumerKey, consumerSecret, accessToken, accessTokenSecret}.
  *
  * @constructor create a new Twitter stream using the supplied Twitter4J authentication credentials.
  */
class TwitterInputDStream(@transient ssc: StreamingContext,
                          twitterAuth: Option[Authorization],
                          filterQuery: Option[FilterQuery],
                          storageLevel: StorageLevel
                         ) extends ReceiverInputDStream[Status](ssc) {

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  override def getReceiver(): Receiver[Status] = {
    new TwitterReceiver(authorization, filterQuery, storageLevel)
  }

}
