package com.aluxian.tweeather.utils

import java.io.Closeable

abstract class CloseableWrapper[T](val value: T) extends Closeable {}
