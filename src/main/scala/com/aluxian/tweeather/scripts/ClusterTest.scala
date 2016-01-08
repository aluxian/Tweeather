package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

object ClusterTest extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    val data = sc.parallelize(Seq("a", "b", "c"))
    data.saveAsTextFile("/tw/test/abc.txt")

    logInfo("Test finished")
    sc.stop()
  }

}
