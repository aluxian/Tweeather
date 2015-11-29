package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils

object TwitterHoseFireTrainer extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Prepare data sets
    val data = MLUtils.loadLibSVMFile(sc, "/tw/fire/data.libsvm").cache()
    val Array(trainingData, testData) = data.toDF().randomSplit(Array(0.9, 0.1))

    // Set input/output neurons based on the data sets
    val inputs = data.map(_.features.size).max()
    val outputs = data.map(_.label).max().toInt

    // Configure the perceptron
    val perceptron = new MultilayerPerceptronClassifier()
      .setLayers(Array(inputs, 5, outputs))
      .setMaxIter(50)

    // Train the perceptron
    val model = perceptron.fit(trainingData)

    // Test the model precision
    val predicted = model.transform(testData).select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("precision")
    logInfo(s"Precision: ${evaluator.evaluate(predicted)}")

    // Save the model
    val output = new ObjectOutputStream(hdfs.create(new Path("/tw/fire/fire.model")))
    output.writeObject(model)
    output.close()
  }

}
