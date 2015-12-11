package com.aluxian.tweeather.transformers

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{BasicParamsReadable, BasicParamsWritable, Identifiable}
import org.apache.spark.sql.types._

/**
  * A feature transformer that replaces urls, @usernames and repeated letters thus reducing the feature space.
  */
class FeatureReducer(override val uid: String)
  extends UnaryTransformer[String, String, FeatureReducer] with BasicParamsWritable {

  def this() = this(Identifiable.randomUID("featureReducer"))

  override protected def createTransformFunc: String => String = {
    raw =>
      val str = raw.toLowerCase()
        .replaceAll("https?:\\/\\/\\S*", "URL") // urls
        .replaceAll("\\B@\\w*", "USERNAME") // @ mentions

      // Repeated letters
      "abcdefghijklmnopqrstuvwxyz".map(_.toString).fold(str) {
        (result, c) =>
          result.replaceAll(s"($c){2,}", s"$c$c")
      }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = StringType

  override def copy(extra: ParamMap): FeatureReducer = defaultCopy(extra)

}

object FeatureReducer extends BasicParamsReadable[FeatureReducer] {
  override def load(path: String): FeatureReducer = super.load(path)
}
