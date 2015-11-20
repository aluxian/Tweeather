package com.aluxian.tweeather.transformers

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.util.Identifiable

class TwitterStopWordsRemover(override val uid: String) extends StopWordsRemover(uid) {

  val twitterStopWords = Seq(
    "RT"
  )

  def this() = {
    this(Identifiable.randomUID("twitterStopWords"))
    setStopWords($(stopWords) ++ twitterStopWords)
  }

}
