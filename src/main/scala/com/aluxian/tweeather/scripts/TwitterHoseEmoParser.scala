package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.models.LabeledText
import org.apache.spark.Logging

object TwitterHoseEmoParser extends Script with Logging {

  val positiveEmoticons = TwitterHoseEmoCollector.positiveEmoticons
  val negativeEmoticons = TwitterHoseEmoCollector.negativeEmoticons

  override def main(args: Array[String]) {
    super.main(args)

    val data = sc.textFile("/tw/sentiment/emo/collected/text*")
      .filter(!_.contains("RT"))
      .map(text => {
        val hasPositive = positiveEmoticons.exists(text.contains)
        val hasNegative = negativeEmoticons.exists(text.contains)
        if (hasPositive ^ hasNegative) LabeledText(text, hasPositive.toDouble) else null
      })
      .filter(_ != null)


    val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1))

    testData.toDF("raw_text", "label").write.save("/tw/sentiment/emo/test.parquet")
    trainingData.toDF("raw_text", "label").write.save("/tw/sentiment/emo/training.parquet")
  }

}
