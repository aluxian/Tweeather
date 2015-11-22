package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.scripts.base.{Hdfs, SparkScript}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object TwitterFireHoseTrainer extends SparkScript with Hdfs with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)

    // Prepare data sets
    val testData = sqlContext.read.parquet(hdfs"/tw/fire/test.parquet")
    val trainingData = sqlContext.read.parquet(hdfs"/tw/fire/training.parquet")

    // Configure the pipeline
    val perceptron = new MultilayerPerceptronClassifier()
      .setLayers(Array(4, 5, 2))
      .setMaxIter(50)

    // Fit the pipeline
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
