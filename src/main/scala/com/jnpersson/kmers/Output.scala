/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers

import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * Output format helper methods
 */
object Output {
  /**
   * Write a data table as TSV to the filesystem.
   * @param data data to write
   * @param writeLocation location to write (prefix name, a suffix will be appended)
   */
  def writeTSV[A](data: Dataset[A], writeLocation: String): Unit =
    data.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")

  /**
   * Write sequences as a FASTA file.
   * @param allKmers data to write (pairs of (sequence, header)
   * @param writeLocation location to write (prefix name, a suffix will be appended)
   */
  def writeFasta(allKmers: Dataset[(NTSeq, String)], writeLocation: String)(implicit spark: SparkSession):
    Unit = {

    import spark.implicits._
    //There is no way to force overwrite with saveAsNewAPIHadoopFile, so delete the data manually
    val hadoopPath = new HPath(writeLocation)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(hadoopPath)) {
      println(s"Deleting pre-existing output path $writeLocation")
      fs.delete(hadoopPath, true)
    }

    allKmers.map(x => (x._2, x._1)).rdd.saveAsNewAPIHadoopFile(writeLocation,
      classOf[String], classOf[NTSeq], classOf[FastaOutputFormat[String, NTSeq]])
  }
}