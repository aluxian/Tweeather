package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.transformers.{ColumnDropper, StringSanitizer, TwitterStopWordsRemover}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

object Sentiment140Trainer extends Script with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)

    // Prepare data sets
    val testData = sqlContext.read.parquet(hdfs"/tw/sentiment140/test.parquet")
    val trainingData = sqlContext.read.parquet(hdfs"/tw/sentiment140/training.parquet")

    // Configure a pipeline
    val pipeline = new Pipeline().setStages(Array(
      new StringSanitizer().setInputCol("raw_text").setOutputCol("text"),
      new Tokenizer().setInputCol("text").setOutputCol("raw_words"),
      new TwitterStopWordsRemover().setInputCol("raw_words").setOutputCol("words"),
      new HashingTF().setInputCol("words").setOutputCol("features"),
      new ColumnDropper().setDropColumns("raw_text", "text", "raw_words", "words"),
      new NaiveBayes().setSmoothing(0.25)
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
    val output = new ObjectOutputStream(hdfs.create(new Path(hdfs"/tw/sentiment/sentiment140.model")))
    output.writeObject(model)
    output.close()
  }

}
