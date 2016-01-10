package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel

import scala.io.Source

/**
  * This script provides a REPL (read-eval-print-loop) interface for the sentiment analyser model created by
  * [[TwitterEmoTrainer]]. The trainer script must be ran first.
  */
object TwitterEmoRepl extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    println("Loading emo model...")
    sc // dummy call to init the context
    val model = PipelineModel.load("/tw/sentiment/models/emo.model")
    println("Done. Write the sentence you want analysed and press <enter>")

    for (input <- Source.stdin.getLines) {
      val data = sc
        .parallelize(Seq(input), 1)
        .toDF("raw_text")

      model
        .transform(data)
        .show(truncate = false)
    }
  }

}
