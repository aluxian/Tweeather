package com.aluxian.tweeather.transformers

import java.nio.file.Files

import com.aluxian.tweeather.RichSeq
import com.aluxian.tweeather.models.Metric
import com.aluxian.tweeather.utils.{JParam, ParamsReadable, ParamsWritable}
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
class WeatherProvider(override val uid: String) extends Transformer with ParamsWritable {

  def this() = this(Identifiable.randomUID("weatherProvider"))

  /**
    * Param for the column name that holds the GRIB download url.
    * @group param
    */
  final val gribUrlCol: JParam[String] =
    new JParam[String](this, "gribUrlCol", "column name for the GRIB download url")

  /** @group setParam */
  def setGribUrlColumn(column: String): this.type = set(gribUrlCol, column)

  /** @group getParam */
  def getGribUrlColumn: String = $(gribUrlCol)

  setDefault(gribUrlCol -> "grib_url")

  /**
    * Param for the folder path where downloaded GRIB files will be saved.
    * @group param
    */
  final val gribsPath: JParam[String] =
    new JParam[String](this, "gribsPath", "folder path where downloaded GRIB files will be saved")

  /** @group setParam */
  def setGribsPath(path: String): this.type = set(gribsPath, path)

  /** @group getParam */
  def getGribsPath: String = $(gribsPath)

  setDefault(gribsPath -> "/tmp/gribs/")

  /**
    * Param for the column name that holds the latitude coordinate.
    * @group param
    */
  final val latitudeCol: JParam[String] =
    new JParam[String](this, "latitudeCol", "column name for the latitude coordinate")

  /** @group setParam */
  def setLatitudeColumn(column: String): this.type = set(latitudeCol, column)

  /** @group getParam */
  def getLatitudeColumn: String = $(latitudeCol)

  setDefault(latitudeCol -> "lat")

  /**
    * Param for the column name that holds the longitude coordinate.
    * @group param
    */
  final val longitudeCol: JParam[String] =
    new JParam[String](this, "longitudeCol", "column name for the longitude coordinate")

  /** @group setParam */
  def setLongitudeColumn(column: String): this.type = set(longitudeCol, column)

  /** @group getParam */
  def getLongitudeColumn: String = $(longitudeCol)

  setDefault(longitudeCol -> "lon")

  /**
    * Param for the local FS path where grib files will be temporarily moved (to be read).
    * @group param
    */
  final val localFsPath: JParam[String] =
    new JParam[String](this, "localFsPath", "temporary local FS path")

  /** @group setParam */
  def setLocalFsPath(path: String): this.type = set(localFsPath, path)

  /** @group getParam */
  def getLocalFsPath: String = $(localFsPath)

  setDefault(localFsPath -> Files.createTempDirectory("gfs").toString)

  /**
    * Param for the weather metrics to retrieve.
    * @group param
    */
  final val metrics: JParam[Seq[Metric]] =
    new JParam[Seq[Metric]](this, "metrics", "weather metrics to retrieve")

  /** @group setParam */
  def setMetrics(metricsSeq: Seq[Metric]): this.type = set(metrics, metricsSeq)

  /** @group getParam */
  def getMetrics: Seq[Metric] = $(metrics)

  setDefault(metrics -> Seq(Metric.Temperature, Metric.Pressure, Metric.Humidity))

  @transient
  val gribSets = mutable.Map[String, Map[Metric, GridDatatype]]()

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(gribUrlCol)).dataType
    require(inputType == StringType, s"grib url type must be string type but got $inputType.")

    val columns = $(metrics).map(_.name)
    val columnsString = columns.mkString(", ")

    if (schema.fieldNames.intersect(columns).nonEmpty) {
      throw new IllegalArgumentException(s"Output columns $columnsString already exist.")
    }

    val outputFields = $(metrics).map(m => StructField(m.name, DoubleType, nullable = false))
    StructType(schema.fields ++ outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    $(metrics).mapCompose(dataset)(metric => df => {
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

      df.withColumn(metric.name, t(col($(latitudeCol)), col($(longitudeCol)), col($(gribUrlCol))))
    })
  }

  override def copy(extra: ParamMap): WeatherProvider = defaultCopy(extra)

  private def downloadGrib(sqlc: SQLContext, gribUrl: String): Unit = {
    val hdfs = FileSystem.get(sqlc.sparkContext.hadoopConfiguration)
    val fileName = MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val hdfsPath = new Path($(gribsPath), fileName)
    val localPath = new Path($(localFsPath), fileName)

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

    // TODO: Use only one FS

    // Copy to local FS then read data
    hdfs.copyToLocalFile(hdfsPath, localPath)
    val data = GridDataset.open(localPath.toString)
    gribSets(gribUrl) = $(metrics).map(m => m -> data.findGridDatatype(m.gridName)).toMap
  }

}

object WeatherProvider extends ParamsReadable[WeatherProvider] {
  override def load(path: String): WeatherProvider = super.load(path)
}
