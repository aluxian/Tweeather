package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.ann.MultilayerPerceptron
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

object TwitterHoseFireTrainer extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Prepare data sets
    logInfo("Getting datasets")
    val Array(trainingData, testData) = sqlc.read.parquet("/tw/fire/parsed/data.parquet")
      .map(row => {
        val parts = row.getString(0).split(',').map(_.toDouble)
        val outputs = Vectors.dense(parts.head)
        val inputs = Vectors.dense(parts.tail)
        (inputs, outputs)
      })
      .toDF("input", "output")
      .randomSplit(Array(0.9, 0.1))

    // Configure the perceptron
    val perceptron = new MultilayerPerceptron()
      .setLayers(Array(3, 5, 1))
      .setInputCol("input")
      .setOutputCol("output")

    // Train the perceptron
    logInfo("Training model")
    val model = perceptron.fit(trainingData)

    // Test the model precision
    logInfo("Testing model")
    val predicted = model
      .setOutputCol("predicted")
      .transform(testData)
      .select("output", "predicted")
      .map {
        case Row(output: Vector, predicted: Vector) =>
          (output.toArray.head, predicted.toArray.head)
      }
      .toDF("label", "prediction")

    // The output layer has only 1 value, use a simple RegressionEvaluator
    val accuracy = new RegressionEvaluator().evaluate(predicted)
    logInfo(s"Accuracy: $accuracy")

    // Save the model
    logInfo("Saving model")
    model.write.overwrite().save("/tw/fire/fire.model")
  }

}
