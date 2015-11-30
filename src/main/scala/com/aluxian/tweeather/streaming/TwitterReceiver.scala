package com.aluxian.tweeather.streaming

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._
import twitter4j.auth.Authorization

class TwitterReceiver(twitterAuth: Authorization,
                      filterQuery: Option[FilterQuery],
                      storageLevel: StorageLevel
                     ) extends Receiver[Status](storageLevel) with Logging {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
    newTwitterStream.addListener(new StatusListener {
      def onStatus(status: Status): Unit = {
        store(status)
      }

      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

      def onScrubGeo(l: Long, l1: Long) {}

      def onTrackLimitationNotice(i: Int): Unit = {
        logWarning(s"Twitter stream track limitation notice: $i")
      }

      def onStallWarning(stallWarning: StallWarning): Unit = {
        logWarning(s"Twitter stream stall warning: ${stallWarning.toString}")
      }

      def onException(e: Exception): Unit = {
        logError("Twitter stream error", e)
      }
    })

    if (filterQuery.isDefined) {
      newTwitterStream.filter(filterQuery.get)
    } else {
      newTwitterStream.sample()
    }

    setTwitterStream(newTwitterStream)
    logInfo("Twitter receiver started")
    stopped = false
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
    logInfo("Twitter receiver stopped")
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }

}
