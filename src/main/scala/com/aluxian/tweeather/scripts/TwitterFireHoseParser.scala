package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.scripts.base.{Hdfs, SparkScript}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object TwitterFireHoseParser extends SparkScript with Hdfs with Logging {

  def main(sc: SparkContext) {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.objectFile[(String, Double)](hdfs"/tw/fire/data/*")
    val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1))

    testData.toDF("raw_text", "label").write.save(hdfs"/tw/fire/test.parquet")
    trainingData.toDF("raw_text", "label").write.save(hdfs"/tw/fire/training.parquet")
  }

}
