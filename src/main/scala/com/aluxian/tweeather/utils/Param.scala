package com.aluxian.tweeather.utils

import com.aluxian.tweeather.models.{LocationBox, Metric}
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._

/**
  * Specialized version of [[org.apache.spark.ml.param.Param[LocationBox]]].
  */
class LocationBoxParam(parent: Params, name: String, doc: String, isValid: LocationBox => Boolean)
  extends Param[LocationBox](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, Validators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: LocationBox): ParamPair[LocationBox] = super.w(value)

  override def jsonEncode(value: LocationBox): String = {
    implicit val formats = DefaultFormats
    Serialization.write[LocationBox](value)
  }

  override def jsonDecode(json: String): LocationBox = {
    implicit val formats = DefaultFormats
    parse(json).extract[LocationBox]
  }

}

/**
  * Specialized version of [[org.apache.spark.ml.param.Param[Array[Metric]]]].
  */
class MetricArrayParam(parent: Params, name: String, doc: String, isValid: Array[Metric] => Boolean)
  extends Param[Array[Metric]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, Validators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[Metric]): ParamPair[Array[Metric]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[Metric]): String = {
    implicit val formats = DefaultFormats
    Serialization.write[Array[Metric]](value)
  }

  override def jsonDecode(json: String): Array[Metric] = {
    implicit val formats = DefaultFormats
    parse(json).extract[Array[Metric]]
  }

}

/**
  * Common validation functions for [[Param.isValid]].
  */
object Validators {

  /** Default validation that always returns true */
  def alwaysTrue[T]: T => Boolean = (_: T) => true

}
