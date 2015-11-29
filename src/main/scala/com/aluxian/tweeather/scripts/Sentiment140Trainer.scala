package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.transformers.{ColumnDropper, FeatureReducer, StringSanitizer}
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row

object Sentiment140Trainer extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Prepare data sets
    val testData = sqlc.read.parquet("/tw/sentiment/140/test.parquet")
    val trainingData = sqlc.read.parquet("/tw/sentiment/140/training.parquet")

    // Configure the pipeline
    val pipeline = new Pipeline().setStages(Array(
      new FeatureReducer().setInputCol("raw_text").setOutputCol("reduced_text"),
      new StringSanitizer().setInputCol("reduced_text").setOutputCol("text"),
      new Tokenizer().setInputCol("text").setOutputCol("raw_words"),
      new StopWordsRemover().setInputCol("raw_words").setOutputCol("words"),
      new HashingTF().setInputCol("words").setOutputCol("features"),
      new ColumnDropper().setDropColumns("raw_text", "reduced_text", "text", "raw_words", "words"),
      new NaiveBayes().setSmoothing(0.5).setFeaturesCol("features")
    ))

    // Fit the pipeline
    val model = pipeline.fit(trainingData)

    // Test the model accuracy
    val predicted = model
      .transform(testData)
      .select("prediction", "label")
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }

    val metrics = new BinaryClassificationMetrics(predicted)
    logInfo(s"Test dataset ROC: ${metrics.areaUnderROC()}")
    logInfo(s"Test dataset PR: ${metrics.areaUnderPR()}")
    metrics.unpersist()

    // Save the model
    val output = new ObjectOutputStream(hdfs.create(new Path("/tw/sentiment/140.model")))
    output.writeObject(model)
    output.close()
  }

}
