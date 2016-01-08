package com.aluxian.tweeather.transformers

import java.io.{File, FileOutputStream}
import java.nio.file.Files

import com.aluxian.tweeather.RichArray
import com.aluxian.tweeather.models.Metric
import com.aluxian.tweeather.utils.MetricArrayParam
import org.apache.spark.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{BasicParamsReadable, BasicParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import resource._
import ucar.nc2.dt.grid.GridDataset

import scala.util.hashing.MurmurHash3
import scalaj.http.Http

/**
  * A transformer that retrieves weather forecasts from NOAA.
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
    metrics -> Array(Metric.Pressure, Metric.Temperature, Metric.Humidity),
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
    import dataset.sqlContext
    import dataset.sqlContext.{sparkContext => sc}

    val outputSchema = transformSchema(dataset.schema, logging = true)
    val emptyDataset = sqlContext.createDataFrame(sc.emptyRDD[Row], dataset.schema)

    // Column names
    val latCol = $(latitudeCol)
    val lonCol = $(longitudeCol)
    val urlCol = $(gribUrlCol)
    val gribsDir = $(gribsPath)
    val metricsArray = $(metrics)

    // Extract all the grib download urls
    val gribUrls = dataset
      .select(urlCol)
      .map(_.getString(0))
      .distinct()
      .collect()

    // Group rows with the same url in the same partition
    val groupedDataset = gribUrls.mapCompose(emptyDataset)(url => groups => {
      val sameUrlGroup = dataset.filter(col(urlCol) === url).coalesce(1)
      groups.unionAll(sameUrlGroup)
    })

    // Process each partition
    val rows = groupedDataset
      .mapPartitions { partition =>
        if (!partition.hasNext) {
          // Skip empty partitions
          partition
        } else {
          val rows = partition.toList
          val headRow = rows.head

          // Extract indexes and the download url
          val latIndex = headRow.fieldIndex(latCol)
          val lonIndex = headRow.fieldIndex(lonCol)
          val urlIndex = headRow.fieldIndex(urlCol)
          val gribUrl = headRow.getString(urlIndex)

          // Download and parse the grib file
          val data = WeatherProvider.downloadGrib(gribUrl, gribsDir, metricsArray)
          val datatypes = metricsArray.map { metric =>
            val datatype = data.findGridDatatype(metric.gridName)
            if (datatype == null) {
              logWarning(s"Null datatype found for $metric from $gribUrl")
            }
            metric -> datatype
          }.toMap

          // Process the partition
          val processedRows = rows.map { row =>
            val lat = row.getDouble(latIndex)
            val lon = row.getDouble(lonIndex)

            val metricValues = datatypes.map {
              case (metric, datatype) =>
                val Array(x, y) = datatype.getCoordinateSystem.findXYindexFromLatLon(lat, lon, null)
                datatype.readDataSlice(0, 0, y, x).getDouble(0)
            }.toArray

            Row.merge(row, Row(metricValues: _*))
          }

          data.close()
          processedRows.toIterator
        }
      }

    sqlContext.createDataFrame(rows, outputSchema)
  }

  override def copy(extra: ParamMap): WeatherProvider = defaultCopy(extra)

}

object WeatherProvider extends BasicParamsReadable[WeatherProvider] with Logging {

  override def load(path: String): WeatherProvider = super.load(path)

  /**
    * Download the grib from the given url and return it as a [[GridDataset]].
    */
  private def downloadGrib(gribUrl: String, gribsDir: String, metrics: Seq[Metric]): GridDataset = {
    val fileName = "gfs" + MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val gribFile = new File(gribsDir, fileName)

    // Download file
    if (Files.notExists(gribFile.toPath)) {
      logInfo(s"Downloading ${gribFile.getPath}")
      val res = Http(gribUrl).asBytes
      if (res.isSuccess) {
        gribFile.getParentFile.mkdirs()
        gribFile.createNewFile()

        for (out <- managed(new FileOutputStream(gribFile, false))) {
          out.write(res.body)
        }
      } else {
        logError("Couldn't download grib", new Throwable(s"Got response code ${res.code} for $gribUrl"))
      }
    } else {
      logInfo(s"Grib ${gribFile.getPath} already downloaded")
    }

    // Read and return
    GridDataset.open(gribFile.getAbsolutePath)
  }

}
