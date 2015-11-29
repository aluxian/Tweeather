package com.aluxian.tweeather.utils

import java.io.ObjectInputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.NaiveBayesModel

object SentimentModels {

  def s140Model(implicit sc: SparkContext): NaiveBayesModel = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val inputStream = new ObjectInputStream(hdfs.open(new Path("/tw/sentiment/140.model")))
    inputStream.readObject().asInstanceOf[NaiveBayesModel]
  }

  def emoModel(implicit sc: SparkContext): NaiveBayesModel = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val inputStream = new ObjectInputStream(hdfs.open(new Path("/tw/sentiment/emo.model")))
    inputStream.readObject().asInstanceOf[NaiveBayesModel]
  }

}
