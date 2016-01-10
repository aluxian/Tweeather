package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * This script provides a REPL (read-eval-print-loop) interface for the model created by
  * [[TwitterFireTrainer]]. The trainer script must be ran first.
  */
object TwitterFireRepl extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    println("Loading fire model...")
    sc // dummy call to init the context
    val model = PipelineModel.load("/tw/fire/models/fire.model")
    println("Done. Write the input as <temperature>,<pressure>,<humidity> and press <enter>")

    for (input <- Source.stdin.getLines) {
      val t = udf { (input: String) =>
        val values = input.split(",").map(_.toDouble)
        Vectors.dense(values)
      }

      val data = sc
        .parallelize(Seq(input), 1)
        .toDF("kb_input")
        .withColumn("raw_input", t(col("kb_input")))

      model
        .transform(data)
        .show(truncate = false)
    }
  }

}
