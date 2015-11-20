package com.aluxian.tweeather.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.auth.Authorization
import twitter4j.{FilterQuery, Status}

object TwitterUtils {

  /**
    * Create a input stream that returns tweets received from Twitter.
    * @param ssc         StreamingContext object
    * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
    *                    authorization; this uses the system properties twitter4j.oauth.consumerKey,
    *                    twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
    *                    twitter4j.oauth.accessTokenSecret
    * @param filterQuery A query to filter the tweets, or Nil to return a sample of the tweets
    * @param storageLevel Storage level to use for storing the received objects
    */
  def createStream(ssc: StreamingContext,
                   twitterAuth: Option[Authorization],
                   filterQuery: Option[FilterQuery] = None,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, filterQuery, storageLevel)
  }

  /**
    * Create a input stream that returns tweets received from Twitter using Twitter4J's default
    * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
    * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and twitter4j.oauth.accessTokenSecret.
    *
    * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
    *
    * @param jssc   JavaStreamingContext object
    */
  def createStream(jssc: JavaStreamingContext): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None)
  }

  /**
    * Create a input stream that returns tweets received from Twitter using Twitter4J's default
    * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
    * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and twitter4j.oauth.accessTokenSecret.
    *
    * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
    *
    * @param jssc    JavaStreamingContext object
    * @param filterQuery A query to filter the tweets, or Nil to return a sample of the tweets
    */
  def createStream(jssc: JavaStreamingContext, filterQuery: FilterQuery): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, Some(filterQuery))
  }

  /**
    * Create a input stream that returns tweets received from Twitter using Twitter4J's default
    * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
    * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and twitter4j.oauth.accessTokenSecret.
    *
    * @param jssc         JavaStreamingContext object
    * @param filterQuery      A query to filter the tweets, or Nil to return a sample of the tweets
    * @param storageLevel Storage level to use for storing the received objects
    */
  def createStream(jssc: JavaStreamingContext,
                   filterQuery: FilterQuery,
                   storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, Some(filterQuery), storageLevel)
  }

  /**
    * Create a input stream that returns tweets received from Twitter.
    * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
    *
    * @param jssc        JavaStreamingContext object
    * @param twitterAuth Twitter4J Authorization
    */
  def createStream(jssc: JavaStreamingContext, twitterAuth: Authorization): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth))
  }

  /**
    * Create a input stream that returns tweets received from Twitter.
    * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
    *
    * @param jssc        JavaStreamingContext object
    * @param twitterAuth Twitter4J Authorization
    * @param filterQuery     A query to filter the tweets, or Nil to return a sample of the tweets
    */
  def createStream(jssc: JavaStreamingContext,
                   twitterAuth: Authorization,
                   filterQuery: FilterQuery
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), Some(filterQuery))
  }

  /**
    * Create a input stream that returns tweets received from Twitter.
    *
    * @param jssc         JavaStreamingContext object
    * @param twitterAuth  Twitter4J Authorization object
    * @param filterQuery      A query to filter the tweets, or Nil to return a sample of the tweets
    * @param storageLevel Storage level to use for storing the received objects
    */
  def createStream(jssc: JavaStreamingContext,
                   twitterAuth: Authorization,
                   filterQuery: FilterQuery,
                   storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), Some(filterQuery), storageLevel)
  }

}
