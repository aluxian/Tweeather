package com.aluxian.tweeather.transformers

import com.aluxian.tweeather.utils.{ParamsReadable, ParamsWritable}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._

/**
  * A feature transformer that removes punctuation and symbols from the input text.
  */
class StringSanitizer(override val uid: String)
  extends UnaryTransformer[String, String, StringSanitizer] with ParamsWritable {

  def this() = this(Identifiable.randomUID("stringSanitizer"))

  override protected def createTransformFunc: String => String = {
    _
      .replaceAll("[^a-z0-9\\s]+", "") // punctuation
      .replaceAll("\\s+", " ") // multiple white spaces
      .trim()
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = StringType

  override def copy(extra: ParamMap): StringSanitizer = defaultCopy(extra)

}

object StringSanitizer extends ParamsReadable[StringSanitizer] {
  override def load(path: String): StringSanitizer = super.load(path)
}
