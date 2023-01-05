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

import com.jnpersson.discount.{Abundance, NTSeq}
import com.jnpersson.discount.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


/**
 * A collection of counted k-mers represented in encoded form. Each k-mer is represented individually,
 * making this dataset large if cached or persisted.
 * @param counts Pairs of encoded k-mers and their abundances.
 * @param splitter Splitter for constructing super-mers
 * @param spark the Spark session
 */
class CountedKmers(val counts: Dataset[(Array[Long], Abundance)], splitter: Broadcast[AnyMinSplitter])
                     (implicit spark: SparkSession) {
  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  /** Cache this dataset. This may be expensive when a large amount of distinct k-mers are present.
   */
  def cache(): this.type = { counts.cache(); this }

  /** Unpersist this dataset. */
  def unpersist(): this.type = { counts.unpersist(); this }

  /** Obtain these counts as pairs of k-mer sequence strings and abundances. */
  def withSequences: Dataset[(NTSeq, Abundance)] = {
    val k = splitter.value.k
    counts.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      //The strings generated here are a big source of memory pressure.
      val dec = NTBitArray.fixedSizeDecoder(k)
      xs.map(x => (dec.longsToString(x._1, 0, k), x._2))
    })
  }

  /**
   * Write counted k-mers with sequences as FASTA files to HDFS.
   * The count will be used as the sequence ID of each k-mer.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeFasta(output: String): Unit = {
    Counting.writeFastaCounts(withSequences, output)
  }

  /**
   * Write a table as TSV.
   * This action triggers a computation.
   * @param withKmers Should k-mer sequences be included in the tables?
   * @param output Directory to write to (prefix name)
   */
  def writeTSV(withKmers: Boolean, output: String): Unit = {
    if (withKmers) {
      Counting.writeTSV(withSequences, output)
    } else {
      Counting.writeTSV(counts.map(_._2), output)
    }
  }
}
