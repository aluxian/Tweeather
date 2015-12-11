package com.aluxian.tweeather.transformers

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

import com.aluxian.tweeather.RichArray
import com.aluxian.tweeather.models.Metric
import com.aluxian.tweeather.utils.MetricArrayParam
import org.apache.spark.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{BasicParamsReadable, BasicParamsWritable, Identifiable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ucar.nc2.dt.GridDatatype
import ucar.nc2.dt.grid.GridDataset

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3
import scalaj.http.Http

/**
  * A transformer that retrieves weather from NOAA.
  */
class WeatherProvider(override val uid: String) extends Transformer with BasicParamsWritable {

  def this() = this(Identifiable.randomUID("weatherProvider"))

  /**
    * Param for the column name that holds the GRIB download url.
    * @group param
    */
  final val gribUrlCol: Param[String] =
    new Param[String](this, "gribUrlCol", "column name for the GRIB download url")

  /** @group setParam */
  def setGribUrlColumn(column: String): this.type = set(gribUrlCol, column)

  /** @group getParam */
  def getGribUrlColumn: String = $(gribUrlCol)

  /**
    * Param for the folder path where downloaded GRIB files will be saved.
    * @group param
    */
  final val gribsPath: Param[String] =
    new Param[String](this, "gribsPath", "folder path where downloaded GRIB files will be saved")

  /** @group setParam */
  def setGribsPath(path: String): this.type = set(gribsPath, path)

  /** @group getParam */
  def getGribsPath: String = $(gribsPath)

  /**
    * Param for the weather metrics to retrieve.
    * @group param
    */
  final val metrics: MetricArrayParam =
    new MetricArrayParam(this, "metrics", "weather metrics to retrieve")

  /** @group setParam */
  def setMetrics(metricsSeq: Array[Metric]): this.type = set(metrics, metricsSeq)

  /** @group getParam */
  def getMetrics: Array[Metric] = $(metrics)

  /**
    * Param for the column name that holds the latitude coordinate.
    * @group param
    */
  final val latitudeCol: Param[String] =
    new Param[String](this, "latitudeCol", "column name for the latitude coordinate")

  /** @group setParam */
  def setLatitudeColumn(column: String): this.type = set(latitudeCol, column)

  /** @group getParam */
  def getLatitudeColumn: String = $(latitudeCol)

  /**
    * Param for the column name that holds the longitude coordinate.
    * @group param
    */
  final val longitudeCol: Param[String] =
    new Param[String](this, "longitudeCol", "column name for the longitude coordinate")

  /** @group setParam */
  def setLongitudeColumn(column: String): this.type = set(longitudeCol, column)

  /** @group getParam */
  def getLongitudeColumn: String = $(longitudeCol)

  setDefault(
    gribUrlCol -> "grib_url",
    gribsPath -> new File(sys.props.get("java.io.tmpdir").get, "tweeather").getAbsolutePath,
    metrics -> Array(Metric.Temperature, Metric.Pressure, Metric.Humidity),
    latitudeCol -> "lat",
    longitudeCol -> "lon"
  )

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(gribUrlCol)).dataType
    require(inputType == StringType, s"grib url type must be string type but got $inputType.")

    val columns = $(metrics).map(_.name)
    val columnsString = columns.mkString(", ")

    if (schema.fieldNames.intersect(columns).nonEmpty) {
      throw new IllegalArgumentException(s"Output columns $columnsString already exist.")
    }

    val outputFields = $(metrics).map(m => StructField(m.name, DoubleType, nullable = true))
    StructType(schema.fields ++ outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    $(metrics).mapCompose(dataset)(metric => df => {
      import WeatherProvider.{downloadGrib, gribSets}

      val gribsPathStr = $(gribsPath)
      val metricsArray = $(metrics)

      val t = udf { (lat: Double, lon: Double, gribUrl: String) =>
        gribSets.synchronized {
          if (!gribSets.contains(gribUrl)) {
            downloadGrib(gribUrl, gribsPathStr, metricsArray)
          }
        }

        if (gribSets.contains(gribUrl)) {
          val datatype = gribSets(gribUrl)(metric)
          if (datatype != null) {
            val Array(x, y) = datatype.getCoordinateSystem.findXYindexFromLatLon(lat, lon, null)
            datatype.readDataSlice(0, 0, y, x).getDouble(0)
          } else {
            Double.NaN
          }
        } else {
          Double.NaN
        }
      }

      df.withColumn(metric.name, t(
        col($(latitudeCol)),
        col($(longitudeCol)),
        col($(gribUrlCol))
      ))
    })
  }

  override def copy(extra: ParamMap): WeatherProvider = defaultCopy(extra)

}

object WeatherProvider extends BasicParamsReadable[WeatherProvider] with Logging {

  val gribSets = new ConcurrentHashMap[String, Map[Metric, GridDatatype]]().asScala

  private def downloadGrib(gribUrl: String, gribsPath: String, metrics: Array[Metric]): Unit = {
    val fileName = "gfs" + MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val gribFile = new File(gribsPath, fileName)

    // Download file
    if (Files.notExists(gribFile.toPath)) {
      logInfo(s"Downloading ${gribFile.getPath}")
      val out = new FileOutputStream(gribFile)

      try {
        val lock = out.getChannel.lock()

        try {
          val res = Http(gribUrl).asBytes

          if (res.isSuccess) {
            out.write(res.body)
          } else {
            logError("Couldn't download grib", new Throwable(s"Got response code ${res.code} for $gribUrl"))
          }
        } finally {
          lock.release()
        }
      } finally {
        out.close()
      }
    }

    // Read data from the GRIB file
    val data = GridDataset.open(gribFile.getAbsolutePath)
    gribSets(gribUrl) = metrics.map(m => {
      val dt = data.findGridDatatype(m.gridName)
      if (dt == null) {
        logWarning(s"Null datatype found for $m in $fileName")
      }
      m -> dt
    }).toMap
  }

  override def load(path: String): WeatherProvider = super.load(path)

}
