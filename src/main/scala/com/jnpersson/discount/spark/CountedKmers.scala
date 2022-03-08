/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
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

import com.jnpersson.discount.hash.MinSplitter
import com.jnpersson.discount.bucket.BucketStats
import com.jnpersson.discount.{Abundance, NTSeq}
import com.jnpersson.discount.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import java.nio.ByteBuffer

/**
 * A set of counted k-mers represented in binary form.
 * @param counts Pairs of binary encoded k-mers and their abundances (counts).
 * @param splitter
 * @param spark
 */
class CountedKmers(val counts: Dataset[(Array[Long], Abundance)], splitter: Broadcast[MinSplitter])
                     (implicit spark: SparkSession) {
  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  /** Cache this dataset. Only recommended when the average k-mer abundance is relatively high.
   */
  def cache(): this.type = { counts.cache(); this }

  /** Unpersist this dataset. */
  def unpersist(): this.type = { counts.unpersist(); this }

  /** Apply additional filtering to these k-mer counts, producing a filtered copy.
   * @param f The filter function to apply to abundances, e.g. _ > 100
   */
  def filter(f: Abundance => Boolean): CountedKmers =
    new CountedKmers(counts.filter(x => f(x._2)), splitter)

  /**
   * Obtain these counts as a histogram.
   * @return Pairs of abundances and their frequencies in the dataset.
   */
  def histogram: Dataset[(Abundance, Long)] = {
    counts.toDF("kmer", "value").select("value").
      groupBy("value").count().sort("value").as[(Abundance, Long)]
  }

  /**
   * Obtain these counts as pairs of k-mer sequence strings and abundances.
   * @return
   */
  def withSequences: Dataset[(NTSeq, Abundance)] = {
    val k = splitter.value.k
    counts.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      //The strings generated here are a big source of memory pressure.
      val buffer = ByteBuffer.allocate(k / 4 + 8) //space for up to 1 extra long
      val builder = new StringBuilder(k)
      xs.map(x => (NTBitArray.longsToString(buffer, builder, x._1, 0, k), x._2))
    })
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeHistogram(output: String): Unit = {
    Counting.writeCountsTable(histogram, output)
  }

  /**
   * Write counted k-mers with sequences as FASTA files to HDFS.
   * The count will be used as the sequence ID of each k-mer.
   * @param output Directory to write to (prefix name)
   */
  def writeFasta(output: String): Unit = {
    Counting.writeFastaCounts(withSequences, output)
  }

  /**
   * Write a table as TSV.
   * @param withKmers Should k-mer sequences be included in the tables?
   * @param output Directory to write to (prefix name)
   */
  def writeTSV(withKmers: Boolean, output: String): Unit = {
    if (withKmers) {
      Counting.writeCountsTable(withSequences, output)
    } else {
      Counting.writeCountsTable(counts.map(_._2), output)
    }
  }

  /**
   * Obtain per-partition (bin) statistics.
   */
  def stats: Dataset[BucketStats] = {
    counts.mapPartitions(kmersAbundances => {
      val onlyCounts = kmersAbundances.map(_._2)
      Iterator(BucketStats.collectFromCounts("", onlyCounts))
    })
  }
}
