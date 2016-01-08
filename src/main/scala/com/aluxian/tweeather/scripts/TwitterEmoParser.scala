package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichBoolean
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode

/**
  * This script parses the tweets collected by [[TwitterEmoCollector]].
  * It removes duplicates and tweets which contain both positive and negative emojis.
  * The resulting dataset is coalesced to reduce the number of partitions.
  */
object TwitterEmoParser extends Script with Logging {

  val positiveEmoticons = TwitterEmoCollector.positiveEmoticons
  val negativeEmoticons = TwitterEmoCollector.negativeEmoticons

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")
      .coalesce(sc.defaultParallelism)
      .map(_.stripPrefix("RT").trim)
      .distinct()
      .map(text => {
        val hasPositive = positiveEmoticons.exists(text.contains)
        val hasNegative = negativeEmoticons.exists(text.contains)
        if (hasPositive ^ hasNegative) (text, hasPositive.toDouble) else null
      })
      .filter(_ != null)

    logInfo("Saving text files")
    data.toDF("raw_text", "label").write.mode(SaveMode.Overwrite)
      .parquet("/tw/sentiment/emo/parsed/data.parquet")

    logInfo("Parsing finished")
    sc.stop()
  }

}
