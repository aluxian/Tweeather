package com.aluxian.tweeather.scripts

import java.net.URL
import java.util.zip.ZipInputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

/**
  * This script downloads the Sentiment140 Twitter dataset for academics.
  * The dataset is used by [[Sentiment140Trainer]] and [[TwitterEmoTrainer]].
  *
  * @see http://help.sentiment140.com/for-students/
  */
object Sentiment140Downloader extends Script with Logging {

  val downloadUrl = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"

  override def main(args: Array[String]) {
    super.main(args)

    logInfo(s"Downloading sentiment140 dataset from $downloadUrl")
    val zip = new ZipInputStream(new URL(downloadUrl).openStream())
    val buffer = new Array[Byte](4 * 1024)

    Stream.continually(zip.getNextEntry)
      .takeWhile(_ != null)
      .foreach { entry =>
        val fileName = entry.getName
        val out = hdfs.create(new Path(s"/tw/sentiment/140/downloaded/$fileName"))
        logInfo(s"Downloading $fileName")

        Stream.continually(zip.read(buffer))
          .takeWhile(_ != -1)
          .foreach { count =>
            out.write(buffer, 0, count)
          }

        out.close()
      }

    zip.close()
    logInfo("Downloading finished")
    sc.stop()
  }

}
