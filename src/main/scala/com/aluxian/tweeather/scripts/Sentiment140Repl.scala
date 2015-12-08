package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel

import scala.io.Source

object Sentiment140Repl extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    println("Loading model...")
    sc // dummy call to init the context
    val model = PipelineModel.load("/tw/sentiment/models/140.model")
    println("Done. Write your query and press <enter>")

    for (input <- Source.stdin.getLines) {
      val data = sc
        .parallelize(Seq(input))
        .toDF("raw_text")

      model.transform(data)
        .select("probability", "prediction")
        .foreach(println)
    }
  }

}
