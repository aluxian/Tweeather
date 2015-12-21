package org.apache.spark.streaming

import javax.servlet.http.HttpServletRequest

import org.apache.spark.streaming.StopStreamingTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{Logging, SparkException}

/**
  * Spark Web UI tab used to stop a [[StreamingContext]].
  * This assumes the given SparkContext has enabled its SparkUI.
  */
class StopStreamingTab(val ssc: StreamingContext)
  extends SparkUITab(getSparkUI(ssc), "stop") with Logging {

  def attach() {
    getSparkUI(ssc).attachTab(this)
    //    getSparkUI(ssc).attachHandler(createRedirectHandler("/streaming/stop", "/streaming/", handleStopRequest))
  }

  def detach() {
    getSparkUI(ssc).detachTab(this)
  }

  private def handleStopRequest(request: HttpServletRequest): Unit = {
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

private object StopStreamingTab {
  def getSparkUI(ssc: StreamingContext): SparkUI = {
    ssc.sc.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
