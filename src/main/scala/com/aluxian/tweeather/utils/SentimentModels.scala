package com.aluxian.tweeather.utils

import java.io.ObjectInputStream

import com.aluxian.tweeather.base.Hdfs
import org.apache.spark.ml.classification.NaiveBayesModel

object SentimentModels extends Hdfs {

  def s140Model(): NaiveBayesModel = {
    val inputStream = new ObjectInputStream(hdfs.open(hdfsp"/tw/sentiment/140.model"))
    inputStream.readObject().asInstanceOf[NaiveBayesModel]
  }

  def emoModel(): NaiveBayesModel = {
    val inputStream = new ObjectInputStream(hdfs.open(hdfsp"/tw/sentiment/emo.model"))
    inputStream.readObject().asInstanceOf[NaiveBayesModel]
  }

}
