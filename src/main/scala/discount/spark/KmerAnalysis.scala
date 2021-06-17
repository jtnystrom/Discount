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

import discount._
import discount.hash._
import discount.util.{KmerTable, ZeroNTBitArray}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object KmerAnalysis {

  /**
   * Create a KmerAnalysis object with some practical default settings ([universal] frequency ordering,
   * 0.01 sample fraction, short reads).
   */
  def apply(input: String, k: Int, m: Int, partitions: Int, motifSet: Option[String],
            unifyRC: Boolean = false)(implicit spark: SparkSession):
    KmerAnalysis = {
    val fraction = 0.01
    val reads = SerialRoutines.getReadsFromFiles(input, k, unifyRC, 1000, Some(fraction))
    val space = SerialRoutines.createSampledSpace(reads, m, partitions,
      motifSet)
    apply(input, k, space, partitions, unifyRC)
  }

  /**
   * Create a KmerAnalysis object with the given MotifSpace.
   */
  def apply(input: String, k: Int, space: MotifSpace, partitions: Int,
            unifyRC: Boolean)(implicit spark: SparkSession): KmerAnalysis = {
    val splitter = MotifExtractor(space, k)
    new KmerAnalysis(space, k, spark.sparkContext.broadcast(splitter), unifyRC)
  }

  /**
   * Create a KmerAnalysis object with the minimizer signature ordering
   */
  def signatureOrdering(input: String, k: Int, m: Int, partitions: Int, unifyRC: Boolean)
                       (implicit spark: SparkSession): KmerAnalysis = {
    val template = MotifSpace.ofLength(m)
    val signature = Orderings.minimizerSignatureSpace(template)
    apply(input, k, signature, partitions, unifyRC)
  }
}

/**
 * Convenience methods for simplifying k-mer counting and segment analysis
 * with some practical default settings.
 */
case class KmerAnalysis(space: MotifSpace, k: Int, splitter: Broadcast[ReadSplitter[Motif]], unifyRC: Boolean) {

  def segmentsByHash(inputPath: String)(implicit spark: SparkSession):
    Dataset[(BucketId, Array[ZeroNTBitArray])] = {
    val input = SerialRoutines.getReadsFromFiles(inputPath, k, unifyRC, 1000)
    val segments = SerialRoutines.hashSegments[Motif](input, splitter)
    SerialRoutines.segmentsByHash(segments)
  }

  def counting(min: Option[Abundance], max: Option[Abundance])(implicit spark: SparkSession): Counting[Motif] =
    new Counting(splitter.value, min, max, unifyRC)

  /**
   * In the haystack, find only the buckets that potentially contain k-mers in the "needle" sequences
   * by joining with the latter's hashes.
   * The orientation of the needles will not be normalized even if the index is.
   */
  def findBuckets(haystack: Dataset[(BucketId, Array[ZeroNTBitArray])], needles: Iterable[NTSeq])
                 (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val needleSegments = needles.flatMap(n => SerialRoutines.createHashSegments(n, splitter)).toSeq.toDS
    val needlesByHash = SerialRoutines.segmentsByHash(needleSegments)
    haystack.join(needlesByHash, "hash").
      toDF("hash", "haystack", "needle")
  }

  /**
   * In the hashed buckets "haystack", find only the k-mers also present in the strings
   * "needles".
   * @return The encoded k-mers that were present both in the haystack and in the needles, and their
   *         abundances.
   */
  def findKmerCounts(haystack: Dataset[(BucketId, Array[ZeroNTBitArray])], needles: Iterable[NTSeq])
                    (implicit spark: SparkSession): Dataset[(Array[Long], Abundance)] = {
    import spark.implicits._
    val buckets = findBuckets(haystack, needles).as[(BucketId, Array[ZeroNTBitArray], Array[ZeroNTBitArray])]
    val k = this.k
    buckets.flatMap { case (id, haystack, needle) => {
      val hsCounted = Counting.countsFromSequences(haystack, k, unifyRC)

      //toSeq for equality (doesn't work for plain arrays)
      //note: this could be faster by sorting and traversing two iterators jointly
      val needleTable = KmerTable.fromSegments(needle, k, unifyRC)
      val needleKmers = needleTable.countedKmers.map(_._1)
      val needleSet = mutable.Set() ++ needleKmers.map(_.toSeq)
      hsCounted.filter(h => needleSet.contains(h._1))
    } }
  }
}
