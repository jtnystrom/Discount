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

import com.jnpersson.discount.bucket.BucketStats
import com.jnpersson.discount.{Abundance, NTSeq}
import com.jnpersson.discount.hash.{BucketId, MinSplitter}
import com.jnpersson.discount.util.{KmerTable, ZeroNTBitArray}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{collect_list, count, expr, first}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable

/**
 * A single hashed sequence segment (super-mer) with its minimizer.
 * @param hash The minimizer
 * @param segment The super-mer
 */
final case class HashSegment(hash: BucketId, segment: ZeroNTBitArray)

object GroupedSegments {

  /** Construct HashSegments from a set of reads/sequences
   *
   * @param input The raw sequence data
   * @param spl   Splitter for breaking the sequences into super-mers
   */
  def hashSegments(input: Dataset[NTSeq], spl: Broadcast[MinSplitter])
                  (implicit spark: SparkSession): Dataset[HashSegment] = {
    import spark.sqlContext.implicits._
    for {
      read <- input
      splitter = spl.value
      (_, rank, segment, _) <- splitter.splitEncode(read)
      r = HashSegment(rank, segment)
    } yield r
  }

  /** Construct HashSegments from a single read
   *
   * @param input    The raw sequence
   * @param splitter Splitter for breaking the sequences into super-mers
   */
  def hashSegments(input: NTSeq, splitter: MinSplitter): Iterator[HashSegment] = {
    for {
      (_, rank, segment, _) <- splitter.splitEncode(input)
      r = HashSegment(rank, segment)
    } yield r
  }

  /** Construct GroupedSegments from a set of reads/sequences
   *
   * @param input  The raw sequence data
   * @param method Counting method/pipeline type
   * @param spl    Splitter for breaking the sequences into super-mers
   */
  def fromReads(input: Dataset[NTSeq], method: CountMethod, spl: Broadcast[MinSplitter])
               (implicit spark: SparkSession): GroupedSegments = {
    import spark.sqlContext.implicits._
    val segments = hashSegments(input, spl)
    val grouped = method match {
      case Pregrouped(normalize) =>
        //For the pregroup method, we add RC segments after grouping if normalizing was requested.
        segmentsByHashPregroup(segments.toDF(), normalize, spl)
      case Simple(_) =>
        //For the simple method, any RC segments will have been added at the input stage.
        segmentsByHash(segments.toDF())
    }
    new GroupedSegments(grouped.as[(BucketId, Array[ZeroNTBitArray], Array[Abundance])], spl)
  }

  /** Group segments by hash/minimizer, pre-grouping and counting identical supermers at an early stage,
   * before assigning to buckets. This helps with high redundancy datasets and can greatly reduce the data volume
   * that must be processed by later stages. However, it leads to one extra shuffle, so it may not be the best choice
   * for moderately sized datasets.
   * Reverse complements are optionally added after pregrouping (when we need to normalize k-mer orientation)
   *
   * @param segments Supermers to group
   * @param addRC Whether to add reverse complements
   * @param spl Splitter broadcast
   */
  def segmentsByHashPregroup(segments: DataFrame, addRC: Boolean, spl: Broadcast[MinSplitter])
                    (implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    //Pre-count each identical segment
    val t1 = segments.selectExpr("hash", "segment").groupBy("segment").
      agg(first("hash").as("hash"), count("segment").as("abundance")).
      select("hash", "segment", "abundance")
    val t2 = (if (addRC) {
      t1.as[(BucketId, ZeroNTBitArray, Abundance)].flatMap { x =>
        //Add reverse complements after pre-counting
        //(May lead to shorter segments/super-kmers for the complements, but each k-mer will be duplicated correctly)
        Iterator((x._1, x._2, x._3)) ++ (for {
          (_, hash, segment, _) <- spl.value.splitRead(x._2, reverseComplement = true)
        } yield (hash.toLong, segment, x._3))
      }
    } else {
      t1
    })

    t2.toDF("hash", "segment", "abundance").groupBy("hash").
      agg(collect_list("segment").as("segments"), collect_list("abundance"))
  }

  /** Group segments by hash/minimizer, non-precounted
   *  This straightforward method is more efficient when supermers are not highly repeated in the data
   *  (low redundancy), or when the data is moderately sized. The outputs are compatible with the method above.
   *
   *  @param segments Supermers to group
   */
  def segmentsByHash(segments: DataFrame)(implicit spark: SparkSession): DataFrame =
    segments.selectExpr("hash", "segment").groupBy("hash").
      agg(collect_list("segment").as("segments"), collect_list(expr("1 as abundance")))
}

/** A collection of counted super-mers grouped into bins (by minimizer).
 * Super-mers are segments of length >= k where every k-mer shares the same minimizer.
 *
 * @param segments The super-mers in binary format, together with their abundances.
 * @param splitter The read splitter
 */
class GroupedSegments(val segments: Dataset[(BucketId, Array[ZeroNTBitArray], Array[Abundance])],
                      val splitter: Broadcast[MinSplitter])(implicit spark: SparkSession)  {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  /** Persist the segments in this object */
  def cache(): this.type = { segments.cache(); this }

  /** Unpersist the segments in this object */
  def unpersist(): this.type = { segments.unpersist(); this }

  /** Convert this dataset to human-readable pairs of (minimizer, super-mer string). */
  def superkmerStrings: Dataset[(String, NTSeq)] = {
    val bcSplit = splitter
    segments.map(seg => {
      (bcSplit.value.humanReadable(seg._1),
        seg._2.map(_.toString()).mkString("\n  "))
    })
  }

  /** Write these segments (as pairs of minimizers and strings) to HDFS.
   *
   * @param outputLocation A directory (prefix name) where the super-mers will be stored.
   */
  def writeSupermerStrings(outputLocation: String): Unit = {
    superkmerStrings.write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${outputLocation}_superkmers")
  }

  /** In these grouped segments, find only the buckets that potentially contain k-mers in the query sequences
   * by joining with the latter's hashes.
   * This method will not perform normalization of the query.
   * This method is intended for interactive exploration and may not scale to large queries.
   *
   * @return triples of (hash, haystack segments, needle segments)
   */
  def joinMatchingBuckets(query: Dataset[NTSeq]): Dataset[(BucketId, Array[ZeroNTBitArray], Array[ZeroNTBitArray])] = {
    import spark.implicits._
    val needleSegments = GroupedSegments.fromReads(query, Simple(false), splitter)
    val needlesByHash = needleSegments.segments
    segments.select("hash", "segments").
      join(needlesByHash.select("hash", "segments"), "hash").
      toDF("hash", "haystack", "needle").as[(BucketId, Array[ZeroNTBitArray], Array[ZeroNTBitArray])]
  }

  /** In these grouped segments, find only the k-mers also present in the query.
   * This method is intended for interactive exploration and may not scale to large queries.
   *
   * @param query Sequences to look for (for example, loaded from a FASTA file using [[InputReader]])
   * @param normalize Whether to normalize k-mer orientation before querying
   * @return The encoded k-mers that were present both in the haystack and in the needles, and their
   *         abundances.
   */
  def lookupFromSequence(query: Dataset[NTSeq], normalize: Boolean): CountedKmers = {
    import spark.implicits._
    val buckets = joinMatchingBuckets(query)
    val k = splitter.value.k
    val counts = buckets.flatMap { case (id, haystack, needle) =>
      val hsCounted = Counting.countsFromSequences(haystack, haystack.map(h => 1L), k, normalize)

      //toSeq for equality (doesn't work for plain arrays)
      val needleTable = KmerTable.fromSegments(needle, needle.map(n => 1L), k, normalize)
      val needleKmers = needleTable.countedKmers.map(_._1)
      val needleSet = mutable.Set() ++ needleKmers.map(ArraySeq.unsafeWrapArray(_))
      hsCounted.filter(h => needleSet.contains(ArraySeq.unsafeWrapArray(h._1)))
    }
    new CountedKmers(counts, splitter)
  }

  /** In these grouped segments, find only the k-mers also present in the query.
   * This method is intended for interactive exploration and may not scale to large queries.
   *
   * @param query Sequences to look for
   * @param normalize Whether to normalize k-mer orientation before querying
   * @return The encoded k-mers that were present both in the haystack and in the needles, and their
   *         abundances.
   */
  def lookupFromSequence(query: Iterable[NTSeq], normalize: Boolean): CountedKmers =
    lookupFromSequence(spark.sparkContext.parallelize(query.toSeq).toDS(), normalize)

  /** Helper class for counting k-mers in this set of super-mers.
   * @param minCount Lower bound for counting
   * @param maxCount Upper bound for counting
   * @param filterOrientation Whether to count only k-mers with forward orientation.
   */
  class Counting(minCount: Option[Abundance], maxCount: Option[Abundance], filterOrientation: Boolean) {
    val countFilter = CountFilter(minCount, maxCount)

    /** Obtain per-bucket (bin) statistics. */
    def bucketStats: Dataset[BucketStats] = {
      val k = splitter.value.k
      val f = countFilter
      val bcSplit = splitter
      val normalize = filterOrientation
      segments.map { case (hash, segments, abundances) =>
        val counted = Counting.getCounts(segments, abundances, k, normalize, f)
        val stats = BucketStats.collectFromCounts(bcSplit.value.humanReadable(hash), counted.map(_._2))
        stats.copy(superKmers = segments.length)
      }
    }

    /** Convert these superkmers into counted k-mers. */
    def counts: CountedKmers = {
      val k = splitter.value.k
      val f = countFilter
      val normalize = filterOrientation
      val counts = segments.flatMap {
        case (_, segments, abundances) => Counting.getCounts(segments, abundances, k, normalize, f) }
      new CountedKmers(counts, splitter)
    }

    /** Write per-bucket statistics to HDFS.
     *
     * @param location Directory (prefix name) to write data to
     */
    def writeBucketStats(location: String): Unit = {
      val bkts = bucketStats
      bkts.cache()
      bkts.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${location}_bucketStats")
      Counting.showStats(bkts, Some(location))
      bkts.unpersist()
    }
  }

  /** Obtain a counting object for these superkmers, which allows access to individual k-mer counts.
   *
   * @param minCount          Lower bound for counting
   * @param maxCount          Upper bound for counting
   * @param filterOrientation Whether to filter out reverse oriented k-mers
   */
  def counting(minCount: Option[Abundance] = None, maxCount: Option[Abundance] = None,
               filterOrientation: Boolean = false) =
    new Counting(minCount, maxCount, filterOrientation)

}
