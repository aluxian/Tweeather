package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}
import twitter4j.FilterQuery

object TwitterHoseEmoCollector extends Script with Logging {

  val positiveEmoticons = Seq(":)", ":-)", "=)", ":D")
  val negativeEmoticons = Seq(":(", ":-(")

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Seconds(30))
    val filter = new FilterQuery().language("en").track(positiveEmoticons ++ negativeEmoticons: _*)
    val stream = TwitterUtils.createStream(ssc, None, Some(filter))

    stream
      .map(_.getText)
      .filter(!_.contains("RT"))
      .map(text => (text, positiveEmoticons.exists(text.contains), negativeEmoticons.exists(text.contains)))
      .filter(p => p._2 != p._3)
      .map(p => (p._1, p._2))
      .saveAsObjectFiles(hdfs"/tw/sentiment/emo/data/scraped-")

    ssc.start()
    ssc.awaitTermination()
  }

}
