package com.aluxian.tweeather.scripts

import java.net.URL
import java.util.zip.ZipInputStream

import org.apache.spark.{Logging, SparkContext}

object Sentiment140Downloader extends Script with Hdfs with Logging {

  val downloadUrl = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"

  def main(sc: SparkContext) {
    val zip = new ZipInputStream(new URL(downloadUrl).openStream())
    val buffer = new Array[Byte](8192)

    Stream.continually(zip.getNextEntry)
      .takeWhile(_ != null)
      .foreach(entry => {
        val entryName = entry.getName
        val output = hdfs.create(hdfsp"/tw/sentiment/140/$entryName")
        logInfo(s"Downloading $entryName")

        Stream.continually(zip.read(buffer))
          .takeWhile(_ != -1)
          .foreach(count => {
            output.write(buffer, 0, count)
          })
      })

    zip.close()
  }

}
