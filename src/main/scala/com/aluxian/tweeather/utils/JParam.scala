package com.aluxian.tweeather.utils

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.json4s.JsonAST.JObject
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

class JParam[T <: AnyRef](override val parent: String,
                          override val name: String,
                          override val doc: String,
                          override val isValid: T => Boolean)
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: T) => true)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc)

  implicit val formats = Serialization.formats(NoTypeHints)

  override def jsonEncode(value: T): String = {
    value match {
      case v: Vector => v.toJson
      case x => Serialization.write(x)
    }
  }

  override def jsonDecode(json: String): T = {
    parse(json) match {
      case JObject(v) if {
        val keys = v.map(_._1)
        keys.contains("type") && keys.contains("values")
      } =>
        Vectors.fromJson(json).asInstanceOf[T]
      case x =>
        Serialization.read[Any](json).asInstanceOf[T]
    }
  }

}
