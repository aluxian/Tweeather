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
  final val dropColumns: Param[Seq[String]] =
    new Param[Seq[String]](this, "dropColumns", "columns to be dropped")

  /** @group setParam */
  def setDropColumns(columns: String*): this.type = set(dropColumns, columns)

  /** @group getParam */
  def getDropColumns: Seq[String] = $(dropColumns)

  setDefault(dropColumns -> Seq())

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.diff($(dropColumns)))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    without(dataset, $(dropColumns))
  }

  override def copy(extra: ParamMap): ColumnDropper = defaultCopy(extra)

  private def without(dataset: DataFrame, columns: Seq[String]): DataFrame = {
    if (columns.isEmpty) {
      return dataset
    }

    without(dataset.drop(columns.last), columns.init)
  }

}
