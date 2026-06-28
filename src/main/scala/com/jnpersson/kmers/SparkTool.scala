/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

package com.jnpersson.kmers

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/** A Spark-based tool.
 * @param appName Name of the application */
private[jnpersson] abstract class SparkTool(appName: String) {

  /** Create a SparkSession with the default settings */
    def sparkSession(): SparkSession = {
      val sp = SparkSession.builder().appName(appName).
        enableHiveSupport().
        getOrCreate()

      //BareLocalFileSystem bypasses the need for winutils.exe on Windows and does no harm on other OS's
      //This affects access to file:/ paths (effectively local files)
      sp.sparkContext.hadoopConfiguration.
        setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])
      sp
    }
}

object SparkTool {
  /** Create a new SparkSession with the given number of shuffle partitions */
  def newSession(base: SparkSession, buckets: Int): SparkSession = {
    val session = base.newSession()
    session.conf.set("spark.sql.shuffle.partitions", buckets.toString)
    session
  }
}

//noinspection TypeAnnotation
class SparkConfiguration(args: Array[String])(implicit val spark: SparkSession) extends Configuration(args) {
  val partitions =
    opt[Int](descr = "Number of shuffle partitions/parquet buckets for indexes (default 200)", default = Some(200))

  def inputReader(files: Seq[String], pairedEnd: Boolean = false)(implicit spark: SparkSession) =
    new Inputs(files, k(), maxSequenceLength(), pairedEnd)

  def inputReader(files: Seq[String], k: Int, pairedEnd: Boolean)(implicit spark: SparkSession) =
    new Inputs(files, k, maxSequenceLength(), pairedEnd)

  def minimizerConfig(): MinimizerConfig = {
    requireSuppliedK()
    new MinimizerConfig(k(), parseMinimizerSource, minimizerWidth(), ordering(), sample(), maxSequenceLength(), normalize())
  }

  def finishSetup(): this.type = {
    verify()
    spark.conf.set("spark.sql.shuffle.partitions", partitions())
    this
  }
}
