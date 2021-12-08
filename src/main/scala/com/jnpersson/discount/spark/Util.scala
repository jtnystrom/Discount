/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter

object Util {
  /**
   * Obtain a PrintWriter for an HDFS location
   * @param location
   * @param spark
   * @return
   */
  def getPrintWriter(location: String)(implicit spark: SparkSession): PrintWriter = {
    val hadoopPath = new Path(location)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val file = fs.create(hadoopPath, true)
    new PrintWriter(file)
  }

  /**
   * Write a text file to a HDFS location
   * @param location
   * @param data
   * @param spark
   */
  def writeTextFile(location: String, data: String)(implicit spark: SparkSession) = {
    val writer = getPrintWriter(location)
    try {
      writer.write(data)
    } finally {
      writer.close()
    }
  }
}
