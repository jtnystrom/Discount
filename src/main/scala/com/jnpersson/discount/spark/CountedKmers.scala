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

import com.jnpersson.discount.bucket.ReducibleBucket
import com.jnpersson.discount.{Abundance, Both, NTSeq, Orientation}
import com.jnpersson.discount.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object CountedKmers {

  /**
   * An iterator over all the k-mers in one bucket paired with abundances.
   */
  private def sequenceCountIterator(b: ReducibleBucket, orientation: Orientation, k: Int): Iterator[(NTSeq, Long)] = {
    val dec = NTBitArray.fixedSizeDecoder(k * 2) //larger than the max size needed to fit an entire super-mer

    //Since 0-valued k-mers are not present in the index, but represent gaps in supermers,
    //we have to filter them out here.
    for { (sm, tags) <- b.supermers.iterator zip b.tags.iterator
          supermerSeq = dec.longsToString(sm.data, 0, sm.size)
          (i, count) <- Iterator.range(0, sm.size - k + 1) zip tags.iterator
          if count > 0
          if orientation == Both || sm.sliceIsForwardOrientation(i, k)
          seq = supermerSeq.substring(i, i + k) }
    yield (seq, count.toLong)
  }
}

/**
 * Routines for converting encoded super-mers into individual counted k-mers.
 * @param buckets Super-mer buckets
 * @param orientation orientation filter for k-mers
 * @param splitter Splitter for constructing super-mers
 * @param spark the Spark session
 */
class CountedKmers(buckets: Dataset[ReducibleBucket], orientation: Orientation, splitter: Broadcast[AnyMinSplitter])
                     (implicit spark: SparkSession) {
  import org.apache.spark.sql._
  import spark.sqlContext.implicits._


  /** Obtain these counts as pairs of k-mer sequence strings and abundances. */
  def withSequences: Dataset[(NTSeq, Abundance)] = {
    val k = splitter.value.k
    val or = orientation
    buckets.flatMap(CountedKmers.sequenceCountIterator(_, or, k))
  }

  /**
   * Write counted k-mers with sequences as FASTA files to HDFS.
   * The count will be used as the sequence ID of each k-mer.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeFasta(output: String): Unit = {
    Output.writeFastaCounts(withSequences, output)
  }

  /**
   * Write a table as TSV.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeTSV(output: String): Unit = {
    Output.writeTSV(withSequences, output)
  }
}
