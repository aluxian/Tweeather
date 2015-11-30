package com.aluxian.tweeather.scripts

import java.net.URL
import java.util.zip.ZipInputStream

import com.amazonaws.util.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

object Sentiment140Downloader extends Script with Logging {

  val downloadUrl = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"

  override def main(args: Array[String]) {
    super.main(args)

    logInfo(s"Downloading sentiment140 dataset from $downloadUrl")
    val zip = new ZipInputStream(new URL(downloadUrl).openStream())

    Stream.continually(zip.getNextEntry)
      .takeWhile(_ != null)
      .foreach(entry => {
        val entryName = entry.getName
        val out = hdfs.create(new Path(s"/tw/sentiment/140/downloaded/$entryName"))
        logInfo(s"Downloading $entryName")
        IOUtils.copy(zip, out)
        IOUtils.closeQuietly(out, null)
      })

    IOUtils.closeQuietly(zip, null)
    logInfo("Downloading finished")
  }

}
