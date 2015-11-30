package com.aluxian.tweeather.transformers

import java.nio.file.Files

import com.aluxian.tweeather.RichSeq
import com.aluxian.tweeather.models.Metric
import com.amazonaws.util.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import ucar.nc2.dt.GridDatatype
import ucar.nc2.dt.grid.GridDataset

import scala.collection.mutable
import scala.util.hashing.MurmurHash3
import scalaj.http.Http

/**
  * A transformer that retrieves weather from NOAA.
  */
class WeatherProvider(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("weatherProvider"))

  /**
    * Param for the column name that holds the GRIB download url.
    * @group param
    */
  final val gribUrl: Param[String] =
    new Param[String](this, "gribUrl", "column name for the GRIB download url")

  /** @group setParam */
  def setGribUrlColumn(column: String): this.type = set(gribUrl, column)

  /** @group getParam */
  def getGribUrlColumn: String = $(gribUrl)

  setDefault(gribUrl -> "grib_url")

  /**
    * Param for the folder path where downloaded GRIB files will be saved.
    * @group param
    */
  final val gribsPath: Param[String] =
    new Param[String](this, "gribsPath", "folder path where downloaded GRIB files will be saved")

  /** @group setParam */
  def setGribsPath(column: String): this.type = set(gribsPath, column)

  /** @group getParam */
  def getGribsPath: String = $(gribsPath)

  setDefault(gribsPath -> "/tmp/gribs/")

  /**
    * Param for the column name that holds the latitude coordinate.
    * @group param
    */
  final val latitude: Param[String] =
    new Param[String](this, "latitude", "column name for the latitude coordinate")

  /** @group setParam */
  def setLatitudeColumn(column: String): this.type = set(latitude, column)

  /** @group getParam */
  def getLatitudeColumn: String = $(latitude)

  setDefault(latitude -> "lat")

  /**
    * Param for the column name that holds the longitude coordinate.
    * @group param
    */
  final val longitude: Param[String] =
    new Param[String](this, "longitude", "column name for the longitude coordinate")

  /** @group setParam */
  def setLongitudeColumn(column: String): this.type = set(longitude, column)

  /** @group getParam */
  def getLongitudeColumn: String = $(longitude)

  setDefault(longitude -> "lon")

  val localFsPath = new Path(Files.createTempDirectory("gfs").toUri)
  val gribSets = mutable.Map[String, Map[Metric, GridDatatype]]()
  val metrics = Seq(
    Metric.Temperature,
    Metric.Pressure,
    Metric.Humidity
  )

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(gribUrl)).dataType
    require(inputType == StringType, s"grib url type must be string type but got $inputType.")

    if (schema.fieldNames.intersect(metrics.map(_.name)).nonEmpty) {
      throw new IllegalArgumentException(s"Output columns temperature, pressure and humidity already exist.")
    }

    val outputFields = metrics.map(m => StructField(m.name, DoubleType, nullable = false))
    StructType(schema.fields ++ outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    metrics.mapCompose(dataset)(metric => df => {
      val t = udf { (lat: Double, lon: Double, gribUrl: String) =>
        if (!gribSets.contains(gribUrl)) {
          downloadGrib(dataset.sqlContext, gribUrl)
        }

        if (gribSets.contains(gribUrl)) {
          val datatype = gribSets(gribUrl)(metric)
          val Array(x, y) = datatype.getCoordinateSystem.findXYindexFromLatLon(lat, lon, null)
          datatype.readDataSlice(0, 0, y, x).getDouble(0)
        } else {
          0
        }
      }

      df.withColumn(metric.name, t(col($(latitude)), col($(longitude)), col($(gribUrl))))
    })
  }

  override def copy(extra: ParamMap): WeatherProvider = defaultCopy(extra)

  private def downloadGrib(sqlc: SQLContext, gribUrl: String): Unit = {
    val hdfs = FileSystem.get(sqlc.sparkContext.hadoopConfiguration)
    val fileName = MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val hdfsPath = new Path($(gribsPath), fileName)
    val localPath = new Path(localFsPath, fileName)

    // Download file to HDFS
    if (!hdfs.exists(hdfsPath)) {
      Http(gribUrl).exec({ (responseCode, headers, stream) =>
        if (responseCode != 200) {
          logError("Couldn't download grib", new Exception(s"Got response code $responseCode for $gribUrl"))
        } else {
          val out = hdfs.create(hdfsPath, true)
          IOUtils.copy(stream, out)
          IOUtils.closeQuietly(out, null)
        }

        IOUtils.closeQuietly(stream, null)
      })
    }

    // Copy to local FS then read data
    hdfs.copyToLocalFile(hdfsPath, localPath)
    val data = GridDataset.open(localPath.toString)
    gribSets(gribUrl) = metrics.map(m => m -> data.findGridDatatype(m.gridName)).toMap
  }

}
