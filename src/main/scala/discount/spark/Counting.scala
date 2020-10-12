/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

import java.nio.ByteBuffer

import discount._
import discount.bucket.BucketStats
import discount.hash.{BucketId, ReadSplitter}
import discount.spark.SerialRoutines._
import discount.util.BPBuffer.ZeroBPBuffer
import discount.util.BPBuffer
import org.apache.spark.sql.{SparkSession}

import scala.util.Sorting


/**
 * Routines related to k-mer counting and statistics.
 * @param spark
 */
abstract class Counting[H](val spark: SparkSession, spl: ReadSplitter[H],
                           minCount: Option[Long], maxCount: Option[Long]) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)

  def toBucketStats(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats]

  val countFilter = new CountFilter(minCount, maxCount)

  def countKmers(reads: Dataset[NTSeq]) = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val counts = segmentsToCounts(segments)
    countedWithSequences(counts)
  }

  def statisticsOnly(reads: Dataset[NTSeq], raw: Boolean): Unit = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val bkts = toBucketStats(segments, raw)
    routines.showStats(bkts)
  }

  /**
   * Only display segment (superkmer) length statistics
   * For benchmarking and testing purposes
   */
  def segmentStatsOnly(reads: Dataset[NTSeq]): Unit = {
    val bcSplit = this.bcSplit
    val k = (bcSplit.value.k - 1)
    val kmersPerSegment = reads.flatMap(r =>
      createHashSegments(r, bcSplit).map(_.segment.size - (k - 1))
    )
    kmersPerSegment.describe().show()
  }

  /**
   * Write per-bucket stats
   * For benchmarking and testing purposes
   * @param reads
   * @param output
   */
  def writeBucketStats(reads: Dataset[NTSeq], output: String): Unit = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val bkts = toBucketStats(segments, false)
    bkts.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_stats")
  }

  /**
   * Convert segments (superkmers) into counted k-mers
   * @param segments
   * @return
   */
  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Long], Long)]

  /**
   * Read inputs, count k-mers and write count tables or histograms
   * @param reads
   * @param withKmers Should k-mer sequences be included in the tables?
   * @param histogram
   * @param output
   */
  def writeCountedKmers(reads: Dataset[NTSeq], withKmers: Boolean, histogram: Boolean, output: String) {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val counts = segmentsToCounts(segments)

    if (histogram) {
      writeCountsTable(countedToHistogram(counts), output)
    } else if (withKmers) {
      writeCountsTable(countedWithSequences(counts), output)
    } else {
      writeCountsTable(counts.map(_._2), output)
    }
  }

  def countedWithSequences(counted: Dataset[(Array[Long], Long)]): Dataset[(NTSeq, Long)] = {
    val k = spl.k
    counted.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      //The strings generated here are a big source of memory pressure.
      val buffer = ByteBuffer.allocate(k / 4 + 8) //space for up to 1 extra long
      val builder = new StringBuilder(k)
      xs.map(x => (BPBuffer.longsToString(buffer, builder, x._1, 0, k), x._2))
    })
  }

  def countedToHistogram(counted: Dataset[(Array[Long], Long)]): Dataset[(Long, Long)] = {
    counted.map(_._2).groupBy("value").count().sort("value").as[(Long, Long)]
  }

  /**
   * Write k-mers and associated counts.
   * @param allKmers
   * @param writeLocation
   */
  def writeCountsTable[A](allKmers: Dataset[A], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")
  }
}

final class SimpleCounting[H](s: SparkSession, spl: ReadSplitter[H],
                              minCount: Option[Long], maxCount: Option[Long])
  extends Counting(s, spl, minCount, maxCount) {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import Counting._

  def uncountedToCounts(segments: Dataset[(BucketId, Array[ZeroBPBuffer])]): Dataset[(Array[Long], Long)] = {
    val k = spl.k
    val f = countFilter
    segments.flatMap { case (hash, segments) => {
      countsFromSequences(segments, k).filter(f.filter)
    } }
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Long], Long)] = {
    uncountedToCounts(
      routines.segmentsByHash(segments, false))
  }

  def toBucketStats(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats] = {
    val k = spl.k
    val f = countFilter
    val byHash = routines.segmentsByHash(segments, false)
    if (raw) {
      byHash.map { case (hash, segments) => {
        //Benchmarking method for degenerate cases.
        //Simply count number of k-mers as a whole (including duplicates)
        //This algorithm should work even when the data is very skewed.
        val totalAbundance = segments.iterator.map(x => x.size.toLong - (k - 1)).sum
        BucketStats(segments.length, totalAbundance, 0, 0, 0)
      } }
    } else {
      byHash.map { case (hash, segments) => {
        val counted = countsFromSequences(segments, k).filter(f.filter)
        val stats = BucketStats.collectFromCounts(counted.map(_._2))
        stats.copy(sequences = segments.length)
      } }
    }
  }
}

/**
 * Min/max abundance filtering for k-mer counts
 * @param min
 * @param max
 */
final case class CountFilter(min: Option[Long], max: Option[Long]) {
  val active = min.nonEmpty || max.nonEmpty

  def filter(x: (Array[Long], Long)): Boolean = {
    !active ||
      ((min.isEmpty || x._2 >= min.get) &&
        (max.isEmpty || x._2 <= max.get))
  }
}

/**
 * Serialization-safe methods for counting
 */
object Counting {
  final class LongKmerOrdering(k: Int) extends Ordering[Array[Long]] {
    val arrayLength = if (k % 32 == 0) { k / 32 } else { (k / 32) + 1 }

    override def compare(x: Array[Long], y: Array[Long]): Int = {
      var i = 0
      while (i < arrayLength) {
        val a = x(i)
        val b = y(i)
        if (a < b) return -1
        else if (a > b) return 1
        i += 1
      }
      0
    }
  }

  /**
   * From a series of sequences (where k-mers may be repeated),
   * produce an iterator with counted abundances where each k-mer appears only once.
   * @param segments
   * @param k
   * @return
   */
  def countsFromSequences(segments: Iterable[BPBuffer], k: Int): Iterator[(Array[Long], Long)] = {
    implicit val ordering = new LongKmerOrdering(k)

    val byKmer = segments.iterator.flatMap(s =>
      s.kmersAsLongArrays(k)
    ).toArray
    Sorting.quickSort(byKmer)

    new Iterator[(Array[Long], Long)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.length

      def hasNext = i < len

      def next = {
        val lastKmer = byKmer(i)
        var count = 0L
        while (i < len && java.util.Arrays.equals(byKmer(i), lastKmer)) {
          count += 1
          i += 1
        }

        (lastKmer, count)
      }
    }
  }
}