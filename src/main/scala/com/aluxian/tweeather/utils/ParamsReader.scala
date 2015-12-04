package com.aluxian.tweeather.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

trait ParamsReadable[T] extends MLReadable[T] {
  override def read: MLReader[T] = new ParamsReader
}

/**
  * [[MLWriter]] implementation for transformers and estimators that contain json4s-serializable params.
  * @tparam T ML instance type
  */
class ParamsReader[T] extends MLReader[T] {
  override def load(path: String): T = {
    val metadata = ParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance = cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    ParamsReader.getAndSetParams(instance, metadata)
    instance.asInstanceOf[T]
  }
}

object ParamsReader {

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * All info from metadata file.
    * @param params  paramMap, as a [[JValue]]
    * @param metadata  All metadata, including the other fields
    * @param metadataJson  Full metadata file String (for debugging)
    */
  case class Metadata(className: String,
                      uid: String,
                      timestamp: Long,
                      sparkVersion: String,
                      params: JValue,
                      metadata: JValue,
                      metadataJson: String)

  /**
    * Load metadata from file.
    * @param expectedClassName  If non empty, this is checked against the loaded metadata.
    * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
    */
  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = ""): Metadata = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    val metadata = parse(metadataStr)

    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val timestamp = (metadata \ "timestamp").extract[Long]
    val sparkVersion = (metadata \ "sparkVersion").extract[String]
    val params = metadata \ "paramMap"

    if (expectedClassName.nonEmpty) {
      require(className == expectedClassName, s"Error loading metadata: Expected class name" +
        s" $expectedClassName but found class name $className")
    }

    Metadata(className, uid, timestamp, sparkVersion, params, metadata, metadataStr)
  }

  /**
    * Extract Params from metadata, and set them in the instance.
    */
  def getAndSetParams(instance: Params, metadata: Metadata): Unit = {
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach {
          case (paramName: String, paramObj: JObject) =>
            val param = instance.getParam(paramName)
            val value = decodeParam(paramObj)
            instance.set(param, value)
          case _ =>
            throw new IllegalArgumentException("Param value should be a JSON object.")
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  /**
    * Decode a parameter JSON object which contains its value and type information.
    */
  def decodeParam(param: JObject): Any = {
    println(param)
    println(manifestFromJson(param))
    (param \ "value").extract(formats, manifestFromJson(param))
  }

  /**
    * Deserialize a [[Manifest]] object from JSON.
    */
  def manifestFromJson(obj: JObject): Manifest[_] = {
    val cls = Class.forName((obj \ "class").extract[String])
    val args = (obj \ "typeArgs").children.map { p =>
      p match {
        case obj: JObject => manifestFromJson(obj)
        case _ =>
          throw new IllegalArgumentException("Args array should contain serialized Manifest objects.")
      }
    }

    args.size match {
      case x if x >= 2 => Manifest.classType(cls, args.head, args.tail: _*)
      case x if x == 1 => Manifest.classType(cls, args.head)
      case _ => Manifest.classType(cls)
    }
  }

  /**
    * Load a [[Params]] instance from the given path, and return it.
    * This assumes the instance implements [[MLReadable]].
    */
  def loadParamsInstance[T](path: String, sc: SparkContext): T = {
    val metadata = ParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]].load(path)
  }

}
