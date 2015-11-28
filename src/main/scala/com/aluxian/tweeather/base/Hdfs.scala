package com.aluxian.tweeather.base

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Hdfs {

  private val hdfsHostUrl = sys.props.getOrElse("fs.defaultFS", "hdfs://localhost:9000")

  lazy val hdfs = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", hdfsHostUrl)
    FileSystem.get(hdfsConf)
  }

  implicit class HdfsStringInterpolator(sc: StringContext) {

    def hdfs(args: Any*): String = hdfsHostUrl + sc.s(args: _*)

    def hdfsp(args: Any*): Path = new Path(hdfs(args: _*))

  }

}
