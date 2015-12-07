package org.apache.spark.ml.util

import org.apache.spark.ml.param.Params

/**
  * Extended trait to make the default implementation public.
  */
trait BasicParamsWritable extends DefaultParamsWritable {
  self: Params =>
}

/**
  * Extended trait to make the default implementation public.
  */
trait BasicParamsReadable[T] extends DefaultParamsReadable[T] {}
