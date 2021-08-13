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

package discount.spark

import discount.{Abundance, NTSeq}
import discount.hash.{Motif, MinSplitter}
import discount.util.NTBitArray
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

  /** Cache this dataset. Note: CountedKmers can get very large and it is often preferable to not cache it. */
  def cache(): this.type = { counts.cache(); this }

  /** Unpersist this dataset. */
  def unpersist(): this.type = { counts.unpersist(); this }

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
   * Read inputs, count k-mers and write count tables or histograms
   * @param reads
   * @param withKmers Should k-mer sequences be included in the tables?
   * @param fromHistogram
   * @param output
   */
  def write(withKmers: Boolean,output: String, tsvFormat: Boolean) {
    import Counting._
    if (withKmers) {
      if (tsvFormat) {
        writeCountsTable(withSequences, output)
      } else {
        writeFastaCounts(withSequences, output)
      }
    } else {
      writeCountsTable(counts.map(_._2), output)
    }
  }
}
