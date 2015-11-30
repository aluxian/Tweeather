package com.aluxian.tweeather.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.ml.util._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

trait DefaultParamsWritable extends MLWritable {
  self: Params =>
  override def write: MLWriter = new DefaultParamsWriter(this)
}

/**
  * Default [[MLWriter]] implementation for transformers and estimators that contain basic
  * (json4s-serializable) params and no data. This will not handle more complex params or types with
  * data (e.g., models with coefficients).
  * @param instance object to save
  */
class DefaultParamsWriter(instance: Params) extends MLWriter {
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
  }
}

object DefaultParamsWriter {

  /**
    * Saves metadata + Params to: path + "/metadata"
    * - class
    * - timestamp
    * - sparkVersion
    * - uid
    * - paramMap
    * - (optionally, extra metadata)
    * @param extraMetadata  Extra metadata to be saved at same level as uid, paramMap, etc.
    * @param paramMap  If given, this is saved in the "paramMap" field.
    *                  Otherwise, all [[org.apache.spark.ml.param.Param]]s are encoded using
    *                  [[org.apache.spark.ml.param.Param.jsonEncode()]].
    */
  def saveMetadata(
                    instance: Params,
                    path: String,
                    sc: SparkContext,
                    extraMetadata: Option[JObject] = None,
                    paramMap: Option[JValue] = None): Unit = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]

    val jsonParams = paramMap.getOrElse(render(params.map {
      case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v))
    }.toList))

    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)

    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }

    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = compact(render(metadata))
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

}

trait DefaultParamsReadable[T] extends MLReadable[T] {
  override def read: MLReader[T] = new DefaultParamsReader
}

/**
  * Default [[MLReader]] implementation for transformers and estimators that contain basic
  * (json4s-serializable) params and no data. This will not handle more complex params or types with
  * data (e.g., models with coefficients).
  * @tparam T ML instance type
  *           TODO: Consider adding check for correct class name.
  */
class DefaultParamsReader[T] extends MLReader[T] {
  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance = cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    DefaultParamsReader.getAndSetParams(instance, metadata)
    instance.asInstanceOf[T]
  }
}

object DefaultParamsReader {

  /**
    * All info from metadata file.
    * @param params  paramMap, as a [[JValue]]
    * @param metadata  All metadata, including the other fields
    * @param metadataJson  Full metadata file String (for debugging)
    */
  case class Metadata(
                       className: String,
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

    implicit val format = DefaultFormats
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
    * This works if all Params implement [[org.apache.spark.ml.param.Param.jsonDecode()]].
    */
  def getAndSetParams(instance: Params, metadata: Metadata): Unit = {
    implicit val format = DefaultFormats
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach { case (paramName, jsonValue) =>
          val param = instance.getParam(paramName)
          val value = param.jsonDecode(compact(render(jsonValue)))
          instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  /**
    * Load a [[Params]] instance from the given path, and return it.
    * This assumes the instance implements [[MLReadable]].
    */
  def loadParamsInstance[T](path: String, sc: SparkContext): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]].load(path)
  }

}
