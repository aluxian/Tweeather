package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.transformers.{ColumnDropper, FeatureReducer, StringSanitizer}
import org.apache.spark.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row

/**
  * This script trains a Naive Bayes classifier with the Sentiment140 dataset of tweets.
  * For training, it uses the dataset of 1.6M tweets. For testing, it uses the manually labelled dataset.
  *
  * Before running this script, the dataset must be first downloaded with [[Sentiment140Downloader]] and then parsed
  * with [[Sentiment140Parser]]. After the model is created, it can be tested with [[Sentiment140Repl]].
  */
object Sentiment140Trainer extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Prepare data sets
    logInfo("Getting datasets")
    val testData = sqlc.read.parquet("/tw/sentiment/140/parsed/test.parquet")
    val trainingData = sqlc.read.parquet("/tw/sentiment/140/parsed/training.parquet")

    // Configure the pipeline
    val pipeline = new Pipeline().setStages(Array(
      new FeatureReducer().setInputCol("raw_text").setOutputCol("reduced_text"),
      new StringSanitizer().setInputCol("reduced_text").setOutputCol("text"),
      new Tokenizer().setInputCol("text").setOutputCol("raw_words"),
      new StopWordsRemover().setInputCol("raw_words").setOutputCol("words"),
      new HashingTF().setInputCol("words").setOutputCol("features"),
      // new IDF().setInputCol("raw_features").setOutputCol("features"),
      new NaiveBayes().setSmoothing(0.5).setFeaturesCol("features"),
      new ColumnDropper().setColumns("raw_text", "reduced_text", "text",
        "raw_words", "words", "features")
    ))

    // Fit the pipeline
    logInfo("Training model")
    val model = pipeline.fit(trainingData)

    // Test the model accuracy
    logInfo("Testing model")
    val predicted = model
      .transform(testData)
      .select("prediction", "label")
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }

    val matches = predicted.map({ case (prediction, label) => if (prediction == label) 1 else 0 }).sum()
    logInfo(s"Test dataset accuracy: ${matches / predicted.count()}")

    val metrics = new BinaryClassificationMetrics(predicted)
    logInfo(s"Test dataset ROC: ${metrics.areaUnderROC()}")
    logInfo(s"Test dataset PR: ${metrics.areaUnderPR()}")
    metrics.unpersist()

    // Save the model
    logInfo("Saving model")
    model.write.overwrite().save("/tw/sentiment/models/140.model")

    logInfo("Training finished")
    sc.stop()
  }

}
