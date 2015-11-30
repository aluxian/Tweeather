package com.aluxian.tweeather.transformers

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.aluxian.tweeather.RichDate
import com.aluxian.tweeather.models.{Coordinates, LocationBox}
import com.aluxian.tweeather.utils.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._

/**
  * A transformer that adds a download url for the GRIB file (weather data).
  */
class GribUrlGenerator(override val uid: String)
  extends UnaryTransformer[Long, String, GribUrlGenerator] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("gribUrlGenerator"))

  /**
    * Param for the location box to be used.
    * @group param
    */
  final val locationBox: Param[LocationBox] =
    new Param[LocationBox](this, "locationBox", "location box to be used")

  /** @group setParam */
  def setLocationBox(box: LocationBox): this.type = set(locationBox, box)

  /** @group getParam */
  def getLocationBox: LocationBox = $(locationBox)

  setDefault(locationBox -> LocationBox(Coordinates(-90, -180), Coordinates(90, 180)))

  override protected def createTransformFunc: Long => String = {
    val dateFormatter = new SimpleDateFormat("yyyyMMdd", Locale.US)
    (timestamp: Long) => {
      val date = new Date(timestamp)
      val dateStr = dateFormatter.format(date)
      val quarterOfDay = date.toCalendar.get(Calendar.HOUR_OF_DAY) match {
        case h if h < 6 => "00"
        case h if h < 12 => "06"
        case h if h < 18 => "12"
        case _ => "18"
      }

      val location = $(locationBox)
      "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl" +
        s"?file=gfs.t${quarterOfDay}z.pgrb2.0p25.anl&dir=%2Fgfs.$dateStr$quarterOfDay" +
        s"&lev_2_m_above_ground=on&lev_surface=on&var_PRES=on&var_RH=on&var_TMP=on" +
        s"&subregion=&leftlon=${location.sw.lon.toInt}&rightlon=${location.ne.lon.toInt}" +
        s"&toplat=${location.ne.lat.toInt}&bottomlat=${location.sw.lat.toInt}"
    }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == LongType, s"Input type must be long type but got $inputType.")
  }

  override protected def outputDataType: DataType = StringType

  override def copy(extra: ParamMap): GribUrlGenerator = defaultCopy(extra)

}

object GribUrlGenerator extends DefaultParamsReadable[GribUrlGenerator] {
  override def load(path: String): GribUrlGenerator = super.load(path)
}
