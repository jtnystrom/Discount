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
import com.jnpersson.discount.{Abundance, NTSeq}
import com.jnpersson.discount.hash.{BucketId, MinSplitter, MinimizerPriorities}
import com.jnpersson.discount.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{collect_list, count, explode, expr, first, isnull, udf, when}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}


/**
 * A single hashed sequence segment (super-mer) with its minimizer.
 * @param hash The minimizer
 * @param segment The super-mer
 */
final case class HashSegment(hash: BucketId, segment: NTBitArray)

object GroupedSegments {

  /** Construct HashSegments from a set of reads/sequences
   *
   * @param input The raw sequence data
   * @param spl   Splitter for breaking the sequences into super-mers
   */
  def hashSegments(input: Dataset[NTSeq], spl: Broadcast[AnyMinSplitter])(implicit spark: SparkSession):
    Dataset[HashSegment] = {
    import spark.sqlContext.implicits._
    implicit val enc = Encoders.tuple(Encoders.STRING, Helpers.encoder(spl.value))
    for {
      read <- input
      splitter = spl.value
      (_, rank, segment, _) <- splitter.splitEncode(read)
    } yield HashSegment(rank.toLong, segment)
  }

  /** Construct HashSegments from a single read
   *
   * @param input    The raw sequence
   * @param splitter Splitter for breaking the sequences into super-mers
   */
  def hashSegments(input: NTSeq, splitter: AnyMinSplitter): Iterator[HashSegment] =
    for {
      (_, rank, segment, _) <- splitter.splitEncode(input)
    } yield HashSegment(rank.toLong, segment)

  /** Construct GroupedSegments from a set of reads/sequences
   *
   * @param input  The raw sequence data
   * @param method Counting method/pipeline type
   * @param spl    Splitter for breaking the sequences into super-mers
   */
  def fromReads(input: Dataset[NTSeq], method: CountMethod, normalize: Boolean, spl: Broadcast[AnyMinSplitter])
               (implicit spark: SparkSession): GroupedSegments = {
    import spark.sqlContext.implicits._
    val segments = hashSegments(input, spl)
    val grouped = method match {
      case Pregrouped =>
        //For the pregroup method, we add RC segments after grouping if normalizing was requested.
        segmentsByHashPregroup(segments.toDF, normalize, spl)
      case Simple =>
        //For the simple method, any RC segments will have been added at the input stage.
        segmentsByHash(segments.toDF)
      case Auto => throw new Exception("Please resolve the count method first (Auto not supported)")
    }
    new GroupedSegments(grouped.as[(BucketId, Array[NTBitArray], Array[Abundance])], spl)
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
  def segmentsByHashPregroup[S <: MinSplitter[MinimizerPriorities]](segments: DataFrame, addRC: Boolean, spl: Broadcast[S])
                                                                   (implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    //Pre-count each identical segment
    val t1 = segments.selectExpr("hash", "segment").groupBy("segment").
      agg(first("hash").as("hash"), count("segment").as("abundance")).
      select("hash", "segment", "abundance")
    val t2 = if (addRC) {
      t1.as[(BucketId, NTBitArray, Abundance)].flatMap { x =>
        //Add reverse complements after pre-counting
        //(May lead to shorter segments/super-kmers for the complements, but each k-mer will be duplicated correctly)
        Iterator((x._1, x._2, x._3)) ++ (for {
          (_, hash, segment, _) <- spl.value.splitRead(x._2, reverseComplement = true)
        } yield (hash, segment, x._3))
      }
    } else {
      t1
    }

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

  def bucketIds(splitter: AnyMinSplitter)(implicit spark: SparkSession): Dataset[BucketId] = {
    import spark.sqlContext.implicits._
    if (splitter.priorities.numMinimizers.isEmpty) {
      throw new Exception("This operation is unsupported for these minimizer priorities.")
    }
    spark.range(splitter.priorities.numMinimizers.get).as[BucketId]
  }
}

/** A collection of counted super-mers grouped into bins (by minimizer).
 * Super-mers are segments of length >= k where every k-mer shares the same minimizer.
 *
 * Unlike with the Index, every k-mer in the super-mers is guaranteed to be present.
 *
 * @param segments The super-mers in binary format, together with their abundances.
 * @param splitter The read splitter
 */
class GroupedSegments(val segments: Dataset[(BucketId, Array[NTBitArray], Array[Abundance])],
                      val splitter: Broadcast[AnyMinSplitter])(implicit spark: SparkSession)  {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  /** Convert this dataset to human-readable pairs of (minimizer, super-mer string). */
  def superkmerStrings: DataFrame = {
    val bcSplit = splitter
    val hr = udf(x => bcSplit.value.humanReadable(x))
    val ts = udf((xs: Array[NTBitArray]) => xs.map(_.toString))
    segments.select(hr($"hash"), explode(ts($"segments")))
  }

  /** Write these segments (as pairs of minimizers and strings) to HDFS.
   * This action triggers a computation.
   * @param outputLocation A directory (prefix name) where the super-mers will be stored.
   */
  def writeSupermerStrings(outputLocation: String): Unit = {
    superkmerStrings.write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${outputLocation}_superkmers")
  }

  def toReducibleBuckets(filterOrientation: Boolean): Dataset[ReducibleBucket] = {
    val k = splitter.value.k
    val df = segments.toDF("id", "segments", "abundances")
    val makeBucket = udf((id: BucketId, segments: Array[NTBitArray], abundances: Array[Abundance]) =>
      ReducibleBucket.countingCompacted(id, segments, abundances, k, filterOrientation))

    //the ID column will not change when creating ReducibleBucket.
    //Express this fact to allow Spark to preserve the shuffling on the ID column.
    df.select($"id", makeBucket($"id", $"segments", $"abundances").as("bucket")).
      select($"id", $"bucket.supermers".as("supermers"), $"bucket.tags".as("tags")).
      as[ReducibleBucket]
  }

  /** Construct a counting index from the input data in these grouped segments */
  def toIndex(filterOrientation: Boolean, numBuckets: Int = 200): Index = {
    //normalize keys: ensure that each minimizer occurs once, and exactly once, in the index.
    //This allows us to safely perform inner joins later, even for union operations.
    //The range (0, splitter.value.priorities.numMinimizers] corresponds to all minimizer IDs we can encounter.
    val standardKeys = GroupedSegments.bucketIds(splitter.value).
      map(x => ReducibleBucket(x, Array(), Array())).
      toDF("id", "supermers", "tags")

    val buckets = toReducibleBuckets(filterOrientation)
    val joint = standardKeys.as[ReducibleBucket].join(buckets, List("id"), "left").
      toDF("id", "sm1", "tags1", "sm2", "tags2").
      select($"id",
        when(!isnull($"sm2"), $"sm2").otherwise($"sm1").as("supermers"),
        when(!isnull($"tags2"), $"tags2").otherwise($"tags1").as("tags")).
      as[ReducibleBucket]

    new Index(IndexParams(splitter, numBuckets, ""), joint)
  }
}
