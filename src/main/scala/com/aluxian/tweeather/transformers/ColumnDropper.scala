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
  final val columns: StringArrayParam =
    new StringArrayParam(this, "columns", "columns to be dropped")

  /** @group setParam */
  def setColumns(value: String*): this.type = set(columns, value.toArray)

  /** @group setParam */
  def setColumns(value: Array[String]): this.type = set(columns, value)

  /** @group getParam */
  def getColumns: Array[String] = $(columns)

  setDefault(columns -> Array())

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.diff($(columns)))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    $(columns).mapCompose(dataset)(col => _.drop(col))
  }

  override def copy(extra: ParamMap): ColumnDropper = defaultCopy(extra)

}

object ColumnDropper extends BasicParamsReadable[ColumnDropper] {
  override def load(path: String): ColumnDropper = super.load(path)
}
