package com.aluxian.tweeather.scripts

import java.net.URL
import java.util.zip.ZipInputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

object Sentiment140Downloader extends Script with Logging {

  val downloadUrl = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"

  override def main(args: Array[String]) {
    super.main(args)

    logInfo(s"Downloading sentiment140 dataset from $downloadUrl")
    val zip = new ZipInputStream(new URL(downloadUrl).openStream())
    val buffer = new Array[Byte](8 * 1024)

    Stream.continually(zip.getNextEntry)
      .takeWhile(_ != null)
      .foreach(entry => {
        val entryName = entry.getName
        val output = hdfs.create(new Path(s"/tw/sentiment/140/downloaded/$entryName"))
        logInfo(s"Downloading $entryName")

        Stream.continually(zip.read(buffer))
          .takeWhile(_ != -1)
          .foreach(count => {
            output.write(buffer, 0, count)
          })
      })

    zip.close()
    logInfo("Downloading finished")
  }

}
