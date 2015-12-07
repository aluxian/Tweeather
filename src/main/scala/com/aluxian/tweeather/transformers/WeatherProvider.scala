package com.aluxian.tweeather.transformers

import java.io.File
import java.nio.file.{Files, Paths}

import com.aluxian.tweeather.RichArray
import com.aluxian.tweeather.models.Metric
import com.aluxian.tweeather.utils.MetricArrayParam
import com.amazonaws.util.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException
import org.apache.hadoop.ipc.RemoteException
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{BasicParamsReadable, BasicParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import ucar.nc2.dt.GridDatatype
import ucar.nc2.dt.grid.GridDataset

import scala.collection.mutable
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success}
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

  /**
    * Param for the local FS path where grib files will be temporarily moved (to be read).
    * @group param
    */
  final val localFsPath: Param[String] =
    new Param[String](this, "localFsPath", "temporary local FS path")

  /** @group setParam */
  def setLocalFsPath(path: String): this.type = set(localFsPath, path)

  /** @group getParam */
  def getLocalFsPath: String = $(localFsPath)

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

  setDefault(
    gribUrlCol -> "grib_url",
    gribsPath -> "/tmp/gribs/",
    latitudeCol -> "lat",
    longitudeCol -> "lon",
    localFsPath -> new File(sys.props.get("java.io.tmpdir").get, "tweeather").getAbsolutePath,
    metrics -> Array(Metric.Temperature, Metric.Pressure, Metric.Humidity)
  )

  val gribSets = mutable.Map[String, Map[Metric, GridDatatype]]()

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(getGribUrlColumn).dataType
    require(inputType == StringType, s"grib url type must be string type but got $inputType.")

    val columns = getMetrics.map(_.name)
    val columnsString = columns.mkString(", ")

    if (schema.fieldNames.intersect(columns).nonEmpty) {
      throw new IllegalArgumentException(s"Output columns $columnsString already exist.")
    }

    val outputFields = getMetrics.map(m => StructField(m.name, DoubleType, nullable = true))
    StructType(schema.fields ++ outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    getMetrics.mapCompose(dataset)(metric => df => {
      val t = udf { (lat: Double, lon: Double, gribUrl: String) =>
        if (!gribSets.contains(gribUrl)) {
          downloadGrib(gribUrl)
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
        col(getLatitudeColumn),
        col(getLongitudeColumn),
        col(getGribUrlColumn)
      ))
    })
  }

  override def copy(extra: ParamMap): WeatherProvider = defaultCopy(extra)

  private def downloadGrib(gribUrl: String): Unit = {
    val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val hdfs = FileSystem.get(sqlc.sparkContext.hadoopConfiguration)

    val fileName = "gfs" + MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val localPath = new Path(getLocalFsPath, fileName)
    val hdfsPath = new Path(getGribsPath, fileName)

    // Download file to HDFS
    if (!hdfs.exists(hdfsPath)) {
      val hdfsLockPath = new Path(fileName + ".lock")
      val lockAcquired = try {
        hdfs.createNewFile(hdfsLockPath)
        hdfs.deleteOnExit(hdfsLockPath)
      } catch {
        case abce: AlreadyBeingCreatedException => false
        case re: RemoteException => re.unwrapRemoteException() match {
          case abce: AlreadyBeingCreatedException => false
          case ex => throw ex
        }
      }

      if (lockAcquired) {
        // Download file
        logInfo(s"Downloading ${hdfsPath.toString}")
        Http(gribUrl).exec({ (responseCode, headers, stream) =>
          if (responseCode != 200) {
            Failure(new Exception(s"Got response code $responseCode for $gribUrl"))
          } else {
            val out = hdfs.create(hdfsPath, true)
            IOUtils.copy(stream, out)
            IOUtils.closeQuietly(out, null)
            Success()
          }
        }).body match {
          case Failure(e) => logError("Couldn't download grib", e)
          case Success(v) => hdfs.delete(hdfsLockPath, true)
        }
      } else {
        // File is already being downloaded
        do {
          synchronized {
            wait(1000) // 1 second
          }
        } while (!hdfs.exists(hdfsPath) || hdfs.exists(hdfsLockPath))
      }
    }

    // Copy to local FS
    if (Files.notExists(Paths.get("file://" + localPath.toString))) {
      hdfs.copyToLocalFile(hdfsPath, localPath)
    }

    // Read data from the GRIB file
    val data = GridDataset.open(localPath.toString)
    println(data.getDataVariables)
    gribSets(gribUrl) = getMetrics.map(m => {
      val dt = data.findGridDatatype(m.gridName)
      if (dt == null) {
        logWarning(s"Null datatype found for $m")
      }
      m -> dt
    }).toMap
  }

}

object WeatherProvider extends BasicParamsReadable[WeatherProvider] {
  override def load(path: String): WeatherProvider = super.load(path)
}
