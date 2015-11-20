package com.aluxian.tweeather.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * A transformer that removes columns.
  */
class ColumnDropper(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("columnsDropper"))

  /**
    * Param for the column names to be removed.
    * @group param
    */
  final val removedColumns: Param[Seq[String]] =
    new Param[Seq[String]](this, "removedColumns", "columns to be removed")

  /** @group setParam */
  def setRemovedColumns(columns: String*): this.type = set(removedColumns, columns)

  /** @group getParam */
  def getRemovedColumns: Seq[String] = $(removedColumns)

  setDefault(removedColumns -> Seq())

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.diff($(removedColumns)))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    without(dataset, $(removedColumns))
  }

  override def copy(extra: ParamMap): ColumnDropper = defaultCopy(extra)

  def without(dataset: DataFrame, columns: Seq[String]): DataFrame = {
    if (columns.isEmpty) {
      return dataset
    }

    without(dataset.drop(columns.last), columns.init)
  }

}
