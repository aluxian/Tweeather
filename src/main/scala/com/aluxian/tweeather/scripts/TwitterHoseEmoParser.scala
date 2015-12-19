package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichBoolean
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode

object TwitterHoseEmoParser extends Script with Logging {

  val positiveEmoticons = TwitterHoseEmoCollector.positiveEmoticons
  val negativeEmoticons = TwitterHoseEmoCollector.negativeEmoticons

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")
      .distinct()
      .filter(!_.contains("RT"))
      .map(text => {
        val hasPositive = positiveEmoticons.exists(text.contains)
        val hasNegative = negativeEmoticons.exists(text.contains)
        if (hasPositive ^ hasNegative) (text, hasPositive.toDouble) else null
      })
      .filter(_ != null)

    logInfo("Saving text files")
    data.toDF("raw_text", "label").write.mode(SaveMode.Overwrite)
      .parquet("/tw/sentiment/emo/parsed/data.parquet")
  }

}
