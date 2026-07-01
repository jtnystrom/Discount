/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.spark

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.discount.bucket.ReducibleBucket
import com.jnpersson.discount.spark.CountedKmers.findNonzeroIntervals
import com.jnpersson.kmers.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.tailrec

object CountedKmers {

  /**
   * An iterator over all the k-mers in one bucket paired with abundances.
   */
  private def sequenceCountIterator(b: ReducibleBucket, orientation: Orientation, k: Int): Iterator[(NTSeq, Long)] = {
    val dec = NTBitArray.decoder
    //TODO change this or back out the previous change that makes super-mers longer

    //Since 0-valued k-mers are not present in the index, but represent gaps in supermers,
    //we have to filter them out here.
    for { (sm, tags) <- b.supermers.iterator zip b.tags.iterator
          supermerSeq = dec.toString(sm)
          (i, count) <- Iterator.range(0, sm.size - k + 1) zip tags.iterator
          if count > 0
          if orientation == Unchanged || sm.sliceIsForwardOrientation(i, k)
          seq = supermerSeq.substring(i, i + k) }
    yield (seq, count.toLong)
  }

  /** Traverse an array of tags, finding maximally long segments without zeroes.
   * @param tags tags (e.g. k-mer counts)
   * @param from starting index in tags array
   * @param acc accumulator
   * @return pairs of (start, end) where start is inclusive and end is not inclusive
   */
  @tailrec
  def findNonzeroIntervals(tags: Array[Int], from: Int = 0, acc: List[(Int, Int)] = Nil): List[(Int, Int)] = {
    if (from >= tags.length)
      acc
    else if (tags(from) == 0)
      findNonzeroIntervals(tags, from + 1, acc)
    else {
      val last = tags.indexWhere(_ == 0, from)
      val elem = if (last != -1)
        (from, last)
      else
        (from, tags.length)
      findNonzeroIntervals(tags, elem._2, elem :: acc)
    }
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
  import spark.implicits._


  /** Obtain these counts as pairs of k-mer sequence strings and abundances. */
  def withSequences: Dataset[(NTSeq, Abundance)] = {
    val k = splitter.value.k
    val or = orientation
    buckets.flatMap(CountedKmers.sequenceCountIterator(_, or, k))
  }

  /** Obtain these counts as triplets of (minimizer, encoded super-mer, tags for super-mer) */
  def supermersWithTags: Dataset[(BucketId, NTBitArray, Array[Int])] =
    buckets.flatMap(b => (b.supermers zip b.tags).iterator.map(x => (b.id, x._1, x._2)))

  /** Obtain these counts as human-readable triplets of (minimizer, super-mer, tags for super-mer
   *
   * @param withZeroTags whether to include k-mers with zero tags (counts). If false, super-mers will
   *                     be split as required. Even if true, empty super-mers (purely zero tags) will not be included.
   */
  def supermersWithTagsReadable(withZeroTags: Boolean): Dataset[(NTSeq, NTSeq, String)] = {
    val bcSpl = splitter

    buckets.flatMap(b => {
      val humanReadableMinimizer = bcSpl.value.humanReadable(b.id)
      val decoder = NTBitArray.decoder
      val k = bcSpl.value.k

      val filtered = (b.supermers.iterator zip b.tags.iterator).
        filter(_._2.exists(_ != 0))

      if (withZeroTags) {
        filtered.map(x => (humanReadableMinimizer, decoder.toString(x._1), x._2.mkString(" ")))
      } else {
        filtered.flatMap(x => {
          val ints = findNonzeroIntervals(x._2)
          val decoded = decoder.toString(x._1)
          ints.map(interval => {
            (humanReadableMinimizer,
              decoded.substring(interval._1, interval._2 + (k - 1)),
              x._2.slice(interval._1, interval._2).mkString(" "))
          })
        })
      }
    })
  }

  /**
   * Write counted k-mers with sequences as FASTA files to HDFS.
   * The count will be used as the sequence ID of each k-mer.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeFasta(output: String): Unit =
    Output.writeFasta(withSequences.map(x => (x._2.toString, x._1)), output + "_counts")

  /**
   * Output super-mers as FASTA. Triggers a computation.
   * Headers will contain identifier, minimizer, and count of every k-mer.
   * @param output directory to write to (prefix name)
   * @param withZero include zero k-mers (gaps)
   * @param identifier FASTA identifier of every sequence
   */
  def writeSupermersFasta(output: String, withZero: Boolean, identifier: String): Unit =
    Output.writeFasta(
      supermersWithTagsReadable(withZero).map(x =>
      (x._2, s"$identifier ${x._1} ${x._3}")),
      output + "_sm"
    )

  /**
   * Write a table as TSV.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeTSV(output: String): Unit =
    Output.writeTSV(withSequences, output)

  /**
   * Output super-mers as TSV. Triggers a computation.
   * The resulting file will contain minimizer, super-mer, and count of every k-mer.
   * @param output directory to write to (prefix name)
   * @param withZero output zero k-mers (gaps)
   */
  def writeSupermersTSV(output: String, withZero: Boolean): Unit =
    Output.writeTSV(supermersWithTagsReadable(withZero), output + "_sm")
}
