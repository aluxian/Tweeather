package com.aluxian.tweeather.utils

object Utils {

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getDefaultClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrDefaultClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getDefaultClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrDefaultClassLoader)
  }

}
