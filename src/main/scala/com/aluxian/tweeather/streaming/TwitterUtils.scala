package com.aluxian.tweeather.streaming

import java.util.Properties

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import twitter4j.auth.{AccessToken, Authorization}
import twitter4j.{FilterQuery, Status, TwitterFactory}

object TwitterUtils {

  /**
    * Create a input stream that returns tweets received from Twitter.
    *
    * @param ssc         StreamingContext object
    * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
    *                    authorization; this uses the system properties twitter4j.oauth.consumerKey,
    *                    twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
    *                    twitter4j.oauth.accessTokenSecret
    * @param filterQuery A query to filter the tweets, or Nil to return just a sample of the tweets
    * @param storageLevel Storage level to use for storing the received objects
    */
  def createStream(ssc: StreamingContext,
                   filterQuery: Option[FilterQuery] = None,
                   twitterAuth: Option[Authorization] = None,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                  ): DStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, filterQuery, storageLevel)
  }

  /**
    * Create multiple input streams that return tweets received from Twitter, and return their union.
    *
    * @param ssc         StreamingContext object
    * @param credentials The [[Authorization]] objects to use for each stream, or None to take them from
    *                    the twitter.properties file
    * @param queryBuilder A function used to build a query to filter each stream, or Nil to return just a sample
    *                     of the tweets
    * @param storageLevel Storage level to use for storing the received objects
    */
  def createMultiStream(ssc: StreamingContext,
                        queryBuilder: () => FilterQuery = () => null,
                        credentials: Seq[Authorization] = loadDefaultCredentials(),
                        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                       ): DStream[Status] = {
    credentials
      .map(auth => createStream(ssc, Some(queryBuilder()), Some(auth)))
      .reduce { (accStream, stream) => accStream.union(stream) }
  }

  private def loadDefaultCredentials(): Seq[Authorization] = {
    val props = loadTwitterProperties()
    val num = props.getProperty("twitter.credentials").toInt
    1.to(num).map(i => {
      val twitter = new TwitterFactory().getInstance()

      twitter.setOAuthConsumer(
        props.getProperty(s"twitter.credentials.$i.consumerKey"),
        props.getProperty(s"twitter.credentials.$i.consumerSecret")
      )

      twitter.setOAuthAccessToken(new AccessToken(
        props.getProperty(s"twitter.credentials.$i.token"),
        props.getProperty(s"twitter.credentials.$i.tokenSecret")
      ))

      twitter.getAuthorization
    })
  }

  private def loadTwitterProperties(): Properties = {
    val properties = new Properties()
    val stream = getClass.getResourceAsStream("/com/aluxian/tweeather/res/twitter.properties")
    properties.load(stream)
    stream.close()
    properties
  }

}
