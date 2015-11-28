package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.base.{Hdfs, SparkScript}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object TwitterHoseFireTrainer extends SparkScript with Hdfs with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare data sets
    val data = MLUtils.loadLibSVMFile(sc, hdfs"/tw/fire/data.libsvm").cache()
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
    val output = new ObjectOutputStream(hdfs.create(hdfsp"/tw/fire/fire.model"))
    output.writeObject(model)
    output.close()
  }

}
