package com.aluxian.tweeather.scripts

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A base trait that all scripts implement. It configures the logger and the [[SparkContext]].
  * Custom, Tweeather-specific env vars:
  *
  * - '''TW_STREAMING_TIMEOUT''' - The period, in seconds, after which streaming should stop.
  * - '''TW_STREAMING_INTERVAL''' - The duration, in seconds, for each streaming batch.
  */
trait Script {

  protected lazy val streamingTimeout = sys.env.get("TW_STREAMING_TIMEOUT") // in seconds
    .map(_.toLong * 1000).getOrElse(-1L)
  protected lazy val streamingInterval = sys.env.get("TW_STREAMING_INTERVAL") // in seconds
    .map(s => Seconds(s.toLong)).getOrElse(Minutes(5))

  protected lazy val scriptName = "Tweeather_" + getClass.getSimpleName.stripSuffix("$")
  protected lazy val sc = new SparkContext(
    new SparkConf()
      .setIfMissing("spark.app.name", scriptName)
      .setIfMissing("spark.streaming.stopGracefullyOnShutdown", "true")
      .setIfMissing("spark.streaming.blockInterval", "30s")
  )

  protected lazy val hdfs = FileSystem.get(sc.hadoopConfiguration)
  protected lazy val sqlc = new SQLContext(sc)

  def main(args: Array[String]) {
    // Log4j properties
    Option(getClass.getResource("/com/aluxian/tweeather/res/log4j.properties")) match {
      case Some(url) => PropertyConfigurator.configure(url)
      case None => System.err.println("Unable to load log4j.properties")
    }
  }

}
