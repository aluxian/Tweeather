package com.aluxian.tweeather.scripts.base

import org.apache.log4j.PropertyConfigurator

trait Script {

  def main(args: Array[String]) {
    // Log4j properties
    Option(getClass.getResource("/com/aluxian/tweeather/log4j.properties")) match {
      case Some(url) => PropertyConfigurator.configure(url)
      case None => System.err.println("Unable to load log4j.properties")
    }
  }

}
