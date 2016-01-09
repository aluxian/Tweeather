package com.aluxian.tweeather.utils

import java.io.{IOException, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object HdfsUtil {

  /** Copy all files in a directory to one output file (merge). */
  @throws(classOf[IOException])
  def copyMergeWithHeader(fs: FileSystem, srcDir: Path, dstFile: Path, conf: Configuration,
                          deleteSource: Boolean = false, addString: String = null, header: String = null): Boolean = {
    if (!fs.getFileStatus(srcDir).isDirectory) {
      return false
    }

    val checkedDstFile = checkDest(srcDir.getName, fs, dstFile, overwrite = false)
    val out: OutputStream = fs.create(checkedDstFile)

    if (header != null) {
      out.write(header.getBytes("UTF-8"))
    }

    try {
      fs.listStatus(srcDir)
        .sortWith(_.compareTo(_) < 0)
        .foreach { content =>
          if (content.isFile) {
            val in = fs.open(content.getPath)
            try {
              IOUtils.copyBytes(in, out, conf, false)
              if (addString != null) {
                out.write(addString.getBytes("UTF-8"))
              }
            } finally {
              in.close()
            }
          }
        }
    } finally {
      out.close()
    }

    if (deleteSource) fs.delete(srcDir, true) else true
  }

  @throws(classOf[IOException])
  private def checkDest(srcName: String, dstFS: FileSystem, dst: Path, overwrite: Boolean): Path = {
    if (dstFS.exists(dst)) {
      val sdst: FileStatus = dstFS.getFileStatus(dst)
      if (sdst.isDirectory) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory")
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite)
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists")
      }
    }
    dst
  }

}
