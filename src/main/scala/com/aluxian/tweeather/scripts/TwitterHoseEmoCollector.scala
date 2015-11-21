package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{Logging, SparkContext}
import twitter4j.FilterQuery

object TwitterHoseEmoCollector extends Script with Logging {

  val positiveEmoticons = Seq(":)", ":-)", "=)", ":D")
  val negativeEmoticons = Seq(":(", ":-(")

  def main(sc: SparkContext) {
    val ssc = new StreamingContext(sc, Minutes(10))
    val filter = new FilterQuery().language("en").track(positiveEmoticons ++ negativeEmoticons: _*)
    val stream = TwitterUtils.createStream(ssc, None, Some(filter))

    stream
      .map(_.getText)
      .filter(!_.contains("RT"))
      .map(text => (text, positiveEmoticons.exists(text.contains), negativeEmoticons.exists(text.contains)))
      .filter(p => p._2 != p._3)
      .map(p => (p._1, if (p._2) 1d else 0d))
      .saveAsTextFiles(hdfs"/tw/sentiment/emo/data/text")

    ssc.start()
    ssc.awaitTermination()
  }

}
