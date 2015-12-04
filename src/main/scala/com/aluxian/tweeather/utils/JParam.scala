package com.aluxian.tweeather.utils

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable

class JParam[T <: AnyRef](override val parent: String,
                          override val name: String,
                          override val doc: String,
                          override val isValid: T => Boolean)
                         (implicit val manifest: Manifest[T])
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean)(implicit manifest: Manifest[T]) =
    this(parent.uid, name, doc, isValid)(manifest)

  def this(parent: String, name: String, doc: String)(implicit manifest: Manifest[T]) =
    this(parent, name, doc, (_: T) => true)(manifest)

  def this(parent: Identifiable, name: String, doc: String)(implicit manifest: Manifest[T]) =
    this(parent.uid, name, doc)(manifest)

}
