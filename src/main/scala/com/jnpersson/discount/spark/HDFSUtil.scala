/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.spark

import org.apache.hadoop.fs.{FSDataInputStream, FileUtil, Path => HPath}
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter
import java.util.Properties
import scala.io.Source

/** HDFS helper routines */
object HDFSUtil {

  /** Is the path absolute? */
  def isAbsolutePath(path: String): Boolean = {
    val p = new HPath(path)
    p.isAbsolute
  }

  /** Qualify the path (e.g. make it absolute if it is relative) */
  def makeQualified(path: String)(implicit spark: SparkSession): String = {
    if (isAbsolutePath(path)) path else {
      val p = new HPath(path)
      val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.makeQualified(p).toString
    }
  }

  /** Does the file exist in HDFS? */
  def fileExists(path: String)(implicit spark: SparkSession): Boolean = {
    val p = new HPath(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(p)
  }

  /** Obtain a PrintWriter for an HDFS location, creating or overwriting a file */
  def getPrintWriter(location: String)(implicit spark: SparkSession): PrintWriter = {
    val hadoopPath = new HPath(location)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val file = fs.create(hadoopPath, true)
    new PrintWriter(file)
  }

  /** Obtain an input stream for an HDFS location */
  def getInputStream(location: String)(implicit spark: SparkSession): FSDataInputStream = {
    val hadoopPath = new HPath(location)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.open(hadoopPath)
  }

  /** Obtain a Source for an HDFS location */
  def getSource(location: String)(implicit spark: SparkSession): Source =
    Source.fromInputStream(getInputStream(location))

  /** Write a text file to a HDFS location */
  def writeTextFile(location: String, data: String)(implicit spark: SparkSession): Unit = {
    val writer = getPrintWriter(location)
    try {
      writer.write(data)
    } finally {
      writer.close()
    }
  }

  /** Write lines of text to a HDFS location */
  def writeTextLines(location: String, lines: Iterator[String])(implicit spark: SparkSession): Unit = {
    val writer = getPrintWriter(location)
    try {
      for { l <- lines } {
        writer.write(l)
        writer.write("\n")
      }
    } finally {
      writer.close()
    }
  }

  /** Write a properties object to a HDFS location */
  def writeProperties(location: String, data: Properties, comment: String)(implicit spark: SparkSession): Unit = {
    val writer = getPrintWriter(location)
    try {
      data.store(writer, comment)
    } finally {
      writer.close()
    }
  }

  /** Read a properties object from a HDFS location */
  def readProperties(location: String)(implicit spark: SparkSession): Properties = {
    val r = new Properties()
    val input = getInputStream(location)
    try {
      r.load(input)
      r
    } finally {
      input.close()
    }
  }

  def deleteRecursive(location: String)(implicit spark: SparkSession): Unit = {
    val hadoopPath = new HPath(location)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(hadoopPath, true)
  }

  /** Copy a file from one path to another */
  def copyFile(from: String, to: String)(implicit spark: SparkSession): Unit = {
    val fromPath = new HPath(from)
    val toPath = new HPath(to)
    val srcFs = fromPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val dstFs = toPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    FileUtil.copy(srcFs, fromPath, dstFs, toPath, false, true, spark.sparkContext.hadoopConfiguration)
  }
}
