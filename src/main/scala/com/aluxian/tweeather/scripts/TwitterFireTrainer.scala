package com.aluxian.tweeather.scripts

import org.apache.spark.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.ann.MultilayerPerceptron
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

/**
  * This script trains a multilayer perceptron with the dataset of tweets
  * collected by [[TwitterFireCollector]] and parsed by [[TwitterFireParser]].
  * It uses 90% of the dataset for training and 10% for testing.
  *
  * After the model is created, it can be tested with [[TwitterFireRepl]].
  */
object TwitterFireTrainer extends Script with Logging {

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
      .toDF("raw_input", "output")
      .randomSplit(Array(0.9, 0.1))

    // Configure the pipeline
    val pipeline = new Pipeline().setStages(Array(
      new MinMaxScaler().setInputCol("raw_input").setOutputCol("input"),
      new MultilayerPerceptron().setLayers(Array(3, 10, 10, 10, 1)).setTol(0.01).setMaxIter(1000)
    ))

    // Train the perceptron
    logInfo(s"Training model on ${trainingData.count()} records")
    val model = pipeline.fit(trainingData)

    // Test the model accuracy
    logInfo("Testing model")
    val predicted = model
      .transform(testData)
      .select("output", "prediction")
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
    model.write.overwrite().save("/tw/fire/models/fire.model")

    logInfo("Training finished")
    sc.stop()
  }

}
