package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichBoolean
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode

import scala.io.StdIn

object TwitterHoseEmoParser extends Script with Logging {

  val positiveEmoticons = TwitterHoseEmoCollector.positiveEmoticons
  val negativeEmoticons = TwitterHoseEmoCollector.negativeEmoticons

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("/tw/sentiment/emo/collected/*.text")

    val reducedPartitionsNum = Math.ceil(data.getNumPartitions / 100d).toInt
    val partitionsNum = Math.max(reducedPartitionsNum, sc.defaultMinPartitions)

    data
      .map(_.stripPrefix("RT").trim)
      .distinct()
      .map(text => {
        val hasPositive = positiveEmoticons.exists(text.contains)
        val hasNegative = negativeEmoticons.exists(text.contains)
        if (hasPositive ^ hasNegative) (text, hasPositive.toDouble) else null
      })
      .filter(_ != null)
      .coalesce(partitionsNum)

    logInfo("Saving text files")
    data.toDF("raw_text", "label").write.mode(SaveMode.Overwrite)
      .parquet("/tw/sentiment/emo/parsed/data.parquet")

    // Pause to keep the web UI running
    logInfo("Press enter to continue")
    StdIn.readLine()
  }

}
