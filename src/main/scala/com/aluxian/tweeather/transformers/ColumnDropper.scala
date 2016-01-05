package com.aluxian.tweeather.transformers

import com.aluxian.tweeather.RichArray
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{BasicParamsReadable, BasicParamsWritable, Identifiable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * A transformer that removes columns.
  */
class ColumnDropper(override val uid: String) extends Transformer with BasicParamsWritable {

  def this() = this(Identifiable.randomUID("columnsDropper"))

  /**
    * Param for the column names to be removed.
    * @group param
    */
  final val dropColumns: StringArrayParam =
    new StringArrayParam(this, "dropColumns", "columns to be dropped")

  /** @group setParam */
  def setDropColumns(columns: String*): this.type = set(dropColumns, columns.toArray)

  /** @group setParam */
  def setDropColumns(columns: Array[String]): this.type = set(dropColumns, columns)

  /** @group getParam */
  def getDropColumns: Array[String] = $(dropColumns)

  setDefault(dropColumns -> Array())

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.diff($(dropColumns)))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    $(dropColumns).mapCompose(dataset)(col => _.drop(col))
  }

  override def copy(extra: ParamMap): ColumnDropper = defaultCopy(extra)

}

object ColumnDropper extends BasicParamsReadable[ColumnDropper] {
  override def load(path: String): ColumnDropper = super.load(path)
}
