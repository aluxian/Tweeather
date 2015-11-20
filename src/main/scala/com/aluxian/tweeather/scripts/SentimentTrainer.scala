package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.transformers.{ColumnDropper, StringSanitizer, TwitterStopWordsRemover}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object SentimentTrainer extends Script with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)

    // Prepare data sets
    val testData = sqlContext.read.parquet(hdfs"/tw/sentiment/data/test.parquet")
    val trainingData = sqlContext.read.parquet(hdfs"/tw/sentiment/data/training.parquet")

    // Configure the pipeline
    val pipeline = new Pipeline().setStages(Array(
      new StringSanitizer().setInputCol("raw_text").setOutputCol("text"),
      new Tokenizer().setInputCol("text").setOutputCol("raw_words"),
      new TwitterStopWordsRemover().setInputCol("raw_words").setOutputCol("words"),
      new HashingTF().setInputCol("words").setOutputCol("features"),
      new ColumnDropper().setRemovedColumns("raw_text", "text", "raw_words", "words"),
      new NaiveBayes().setSmoothing(0.25)
    ))

    // Fit the pipeline
    val model = pipeline.fit(trainingData)

    // Test the model
    val evaluator = new BinaryClassificationEvaluator()
    val accuracy = evaluator.evaluate(model.transform(testData))
    logDebug(s"Test dataset accuracy: $accuracy")

    // Save the model
    val output = new ObjectOutputStream(hdfs.create(new Path(hdfs"/tw/sentiment/nb.model")))
    output.writeObject(model)
    output.close()
  }

}
