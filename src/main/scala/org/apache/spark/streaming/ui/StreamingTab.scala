package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.ui.StreamingTab._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{Logging, SparkException}

/**
  * Spark Web UI tab that shows statistics of a streaming job.
  * This assumes the given SparkContext has enabled its SparkUI.
  */
private[spark] class StreamingTab(val ssc: StreamingContext)
  extends SparkUITab(getSparkUI(ssc), "streaming") with Logging {

  private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"
  private val STATIC_HANDLER_PATH = "/static/streaming"

  val parent = getSparkUI(ssc)
  val listener = ssc.progressListener

  ssc.addStreamingListener(listener)
  ssc.sc.addSparkListener(listener)
  attachPage(new StreamingPage(this))
  attachPage(new BatchPage(this))

  def attach() {
    getSparkUI(ssc).attachTab(this)
    getSparkUI(ssc).attachHandler(createRedirectHandler("/streaming/stop", "/streaming/", handleStopRequest))
    println("stop handler attached")
    getSparkUI(ssc).addStaticHandler(STATIC_RESOURCE_DIR, STATIC_HANDLER_PATH)
  }

  def detach() {
    getSparkUI(ssc).detachTab(this)
    getSparkUI(ssc).removeStaticHandler(STATIC_HANDLER_PATH)
  }

  private def handleStopRequest(request: HttpServletRequest): Unit = {
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

private object StreamingTab {
  def getSparkUI(ssc: StreamingContext): SparkUI = {
    ssc.sc.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
