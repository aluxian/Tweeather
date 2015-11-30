package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichModel
import org.apache.spark.Logging
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils

object TwitterHoseFireTrainer extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Prepare data sets
    logInfo("Getting datasets")
    val data = MLUtils.loadLibSVMFile(sc, "/tw/fire/parsed/data.libsvm").cache()
    val Array(trainingData, testData) = data.toDF().randomSplit(Array(0.9, 0.1))

    // Set input/output neurons based on the data sets
    val inputs = data.map(_.features.size).max()
    val outputs = data.map(_.label).max().toInt

    // Configure the perceptron
    val perceptron = new MultilayerPerceptronClassifier()
      .setLayers(Array(inputs, 5, outputs))
      .setMaxIter(50)

    // Train the perceptron
    logInfo("Training model")
    val model = perceptron.fit(trainingData)

    // Test the model precision
    logInfo("Testing model")
    val predicted = model.transform(testData).select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("precision")
    logInfo(s"Precision: ${evaluator.evaluate(predicted)}")

    // Save the model
    logInfo("Saving model")
    model.serialize(hdfs, "/tw/fire/fire.model")
  }

}
