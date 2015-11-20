package com.aluxian.tweeather.scripts

import java.io.ObjectOutputStream

import com.aluxian.tweeather.transformers.{ColumnDropper, FeatureReducer, StringSanitizer}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object TwitterHoseEmoTrainer extends Script with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare data set
    val trainingData = sc.objectFile(hdfs"/tw/sentiment/emo/data/*").toDF("raw_text", "label")
    logInfo(s"Training on ${trainingData.count()} tweets")

    // Configure the pipeline
    val pipeline = new Pipeline().setStages(Array(
      new FeatureReducer().setInputCol("raw_text").setOutputCol("reduced_text"),
      new StringSanitizer().setInputCol("reduced_text").setOutputCol("text"),
      new Tokenizer().setInputCol("text").setOutputCol("raw_words"),
      new StopWordsRemover().setInputCol("raw_words").setOutputCol("words"),
      new HashingTF().setInputCol("words").setOutputCol("features"),
      new ColumnDropper().setDropColumns("raw_text", "reduced_text", "text", "raw_words", " words"),
      new NaiveBayes().setSmoothing(0.5)
    ))

    // Fit the pipeline
    val model = pipeline.fit(trainingData)
    logInfo("Training finished")

    // Save the model
    val output = new ObjectOutputStream(hdfs.create(new Path(hdfs"/tw/sentiment/emo.model")))
    output.writeObject(model)
    output.close()
  }

}
