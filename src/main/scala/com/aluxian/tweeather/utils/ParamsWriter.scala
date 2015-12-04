package com.aluxian.tweeather.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.ml.util._
import org.json4s.JsonDSL.WithDouble._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{Extraction, NoTypeHints, _}

trait ParamsWritable extends MLWritable {
  self: Params =>
  override def write: MLWriter = new ParamsWriter(this)
}

/**
  * [[MLWriter]] implementation for transformers and estimators that contain json4s-serializable params.
  * @param instance object to save
  */
class ParamsWriter(instance: Params) extends MLWriter {
  override protected def saveImpl(path: String): Unit = {
    ParamsWriter.saveMetadata(instance, path, sc)
  }
}

object ParamsWriter {

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Saves metadata + Params to: path + "/metadata"
    * - class
    * - timestamp
    * - sparkVersion
    * - uid
    * - paramMap
    */
  def saveMetadata(instance: Params, path: String, sc: SparkContext): Unit = {
    val jsonParams = instance.extractParamMap().toSeq.map {
      case ParamPair(p, v) =>
        val manifest = p match {
          case param: JParam[_] => param.manifest
          case _ => Manifest.classType(v.getClass)
        }
        p.name -> manifestToJson(manifest) ~ ("value" -> Extraction.decompose(v))
    }

    val metadata = JObject(
      "class" -> JString(instance.getClass.getName),
      "timestamp" -> JInt(System.currentTimeMillis()),
      "sparkVersion" -> JString(sc.version),
      "uid" -> JString(instance.uid),
      "paramMap" -> JObject(jsonParams.toList)
    )

    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = compact(render(metadata))

    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  def manifestToJson(manifest: Manifest[_]): JObject = {
    JObject(
      "class" -> JString(manifest.runtimeClass.getName),
      "typeArgs" -> JArray(manifest.typeArguments.map(manifestToJson))
    )
  }

}
