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

import scala.collection.mutable
import java.nio.ByteBuffer
import discount._
import discount.bucket.BucketStats
import discount.hash.{BucketId, ReadSplitter}
import discount.spark.SerialRoutines._
import org.apache.hadoop.fs.{FileSystem, Path}
import discount.util.{NTBitArray, ZeroNTBitArray}
import gov.jgi.meta.hadoop.output.FastaOutputFormat
import org.apache.spark.sql.SparkSession

/**
 * Routines related to k-mer counting and statistics.
 * @param spark
 */
abstract class Counting[H](spl: ReadSplitter[H], minCount: Option[Abundance],
                           maxCount: Option[Abundance])(implicit val spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)

  def toBucketStats(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats]

  val countFilter = new CountFilter(minCount, maxCount)

  def countKmers(reads: Dataset[NTSeq]): Dataset[(NTSeq, Abundance)] = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val counts = segmentsToCounts(segments)
    countedWithSequences(counts)
  }

  def getStatistics(reads: Dataset[NTSeq], raw: Boolean): Dataset[BucketStats] = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    toBucketStats(segments, raw)
  }

  def statisticsOnly(reads: Dataset[NTSeq], raw: Boolean): Unit = {
    SerialRoutines.showStats(getStatistics(reads, raw))
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
    bkts.cache()
    bkts.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_bucketStats")
    SerialRoutines.showStats(bkts)
    bkts.unpersist()
  }

  /**
   * Convert segments (superkmers) into counted k-mers
   * @param segments
   * @return
   */
  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Long], Abundance)]

  /**
   * Read inputs, count k-mers and write count tables or histograms
   * @param reads
   * @param withKmers Should k-mer sequences be included in the tables?
   * @param histogram
   * @param output
   */
  def writeCountedKmers(reads: Dataset[NTSeq], withKmers: Boolean, histogram: Boolean, output: String,
                        tsvFormat: Boolean) {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val counts = segmentsToCounts(segments)

    if (histogram) {
      writeCountsTable(countedToHistogram(counts), output)
    } else if (withKmers) {
      if (tsvFormat) {
        writeCountsTable(countedWithSequences(counts), output)
      } else {
        writeFastaCounts(countedWithSequences(counts), output)
      }
    } else {
      writeCountsTable(counts.map(_._2), output)
    }
  }

  def countedWithSequences(counted: Dataset[(Array[Long], Abundance)]): Dataset[(NTSeq, Abundance)] = {
    val k = spl.k
    counted.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      //The strings generated here are a big source of memory pressure.
      val buffer = ByteBuffer.allocate(k / 4 + 8) //space for up to 1 extra long
      val builder = new StringBuilder(k)
      xs.map(x => (NTBitArray.longsToString(buffer, builder, x._1, 0, k), x._2))
    })
  }

  /**
   * Produce a histogram that maps each abundance value to the number of k-mers with that abundance.
   */
  def countedToHistogram(counted: Dataset[(Array[Long], Abundance)]): Dataset[(Abundance, Long)] = {
    counted.toDF("kmer", "value").select("value").
      groupBy("value").count().sort("value").as[(Abundance, Long)]
  }

  /**
   * Write k-mers and associated counts.
   * @param allKmers
   * @param writeLocation
   */
  def writeCountsTable[A](allKmers: Dataset[A], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")
  }

  /**
   * Write k-mers with counts as FASTA files. Each k-mer becomes a separate sequence.
   * The counts are output as sequence ID headers.
   * @param allKmers
   * @param writeLocation
   */
  def writeFastaCounts(allKmers: Dataset[(NTSeq, Long)], writeLocation: String): Unit = {
    //There is no way to force overwrite with saveAsNewAPIHadoopFile, so delete the data manually
    val outputPath = s"${writeLocation}_counts"
    val hadoopPath = new Path(outputPath)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(hadoopPath)) {
      println(s"Deleting pre-existing output path $outputPath")
      fs.delete(hadoopPath, true)
    }

    allKmers.map(x => (x._2.toString, x._1)).rdd.saveAsNewAPIHadoopFile(outputPath,
      classOf[String], classOf[NTSeq], classOf[FastaOutputFormat[String, NTSeq]])
  }
}


final class SimpleCounting[H](spl: ReadSplitter[H],
                              minCount: Option[Abundance] = None, maxCount: Option[Abundance] = None,
                              filterOrientation: Boolean = false)(implicit spark: SparkSession)
  extends Counting(spl, minCount, maxCount) {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import Counting._

  def groupedToCounts(segments: Dataset[(BucketId, Array[ZeroNTBitArray])]): Dataset[(Array[Long], Abundance)] = {
    val k = spl.k
    val f = countFilter
    val normalize = filterOrientation
    segments.flatMap { case (hash, segments) => {
      countsFromSequences(segments, k, normalize).filter(f.filter)
    } }
  }

  def groupedToHistogram(segments: Dataset[(BucketId, Array[ZeroNTBitArray])]): Dataset[(Abundance, BucketId)] =
    countedToHistogram(groupedToCounts(segments))

  def groupedToCounted(segments: Dataset[(BucketId, Array[ZeroNTBitArray])]): Dataset[(NTSeq, Abundance)] =
    countedWithSequences(groupedToCounts(segments))

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Long], Abundance)] =
    groupedToCounts(SerialRoutines.segmentsByHash(segments))

  def toBucketStats(segments: Dataset[HashSegment], raw: Boolean = false): Dataset[BucketStats] = {
    val byHash = SerialRoutines.segmentsByHash(segments)
    groupedToBucketStats(byHash, raw)
  }

  def groupedToBucketStats(byHash: Dataset[(BucketId, Array[ZeroNTBitArray])],
                           raw: Boolean = false): Dataset[BucketStats] = {
    val k = spl.k
    val f = countFilter
    val bcSplit = this.bcSplit
    val normalize = filterOrientation
    if (raw) {
      byHash.map { case (hash, segments) => {
        //Benchmarking method for degenerate cases.
        //Simply count number of k-mers as a whole (including duplicates)
        //This algorithm should work even when the data is very skewed.
        val totalAbundance = segments.iterator.map(x => x.size.toLong - (k - 1)).sum
        BucketStats(bcSplit.value.humanReadable(hash),
          segments.length, totalAbundance, 0, 0, 0)
      } }
    } else {
      byHash.map { case (hash, segments) => {
        val counted = countsFromSequences(segments, k, normalize).filter(f.filter)
        val stats = BucketStats.collectFromCounts(bcSplit.value.humanReadable(hash),
          counted.map(_._2))
        stats.copy(superKmers = segments.length)
      } }
    }
  }
}

/**
 * Min/max abundance filtering for k-mer counts
 * @param min
 * @param max
 */
final case class CountFilter(min: Option[Abundance], max: Option[Abundance]) {
  val active = min.nonEmpty || max.nonEmpty

  def filter(x: (Array[Long], Abundance)): Boolean = {
    !active ||
      ((min.isEmpty || x._2 >= min.get) &&
        (max.isEmpty || x._2 <= max.get))
  }
}

/**
 * Serialization-safe methods for counting
 */
object Counting {
  def orderingForK(k: Int): Ordering[Array[Long]] = {
    if (k <= 32) {
      LongKmerOrd1
    } else if (k <= 64) {
      LongKmerOrd2
    } else if (k <= 96) {
      LongKmerOrd3
    } else {
      new LongKmerOrdering(k)
    }
  }

  /**
   * Specialised ordering for k <= 32
   */
  object LongKmerOrd1 extends Ordering[Array[Long]] {
    override def compare(x: Array[Long], y: Array[Long]): Int = {
      val a = x(0)
      val b = y(0)
      if (a < b) return -1
      else if (a > b) return 1
      0
    }
  }

  /**
   * Specialised ordering for k <= 64
   */
  object LongKmerOrd2 extends Ordering[Array[Long]] {
    override def compare(x: Array[Long], y: Array[Long]): Int = {
      var a = x(0)
      var b = y(0)
      if (a < b) return -1
      else if (a > b) return 1
      a = x(1)
      b = y(1)
      if (a < b) return -1
      else if (a > b) return 1
      0
    }
  }

  /**
   * Specialised ordering for k <= 96
   */
  object LongKmerOrd3 extends Ordering[Array[Long]] {
    override def compare(x: Array[Long], y: Array[Long]): Int = {
      var a = x(0)
      var b = y(0)
      if (a < b) return -1
      else if (a > b) return 1
      a = x(1)
      b = y(1)
      if (a < b) return -1
      else if (a > b) return 1
      a = x(2)
      b = y(2)
      if (a < b) return -1
      else if (a > b) return 1
      0
    }
  }

  /**
   * General ordering for any k
   */
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
  def countsFromSequences(segments: Iterable[NTBitArray], k: Int,
                          forwardOnly: Boolean): Iterator[(Array[Long], Abundance)] = {
    implicit val ordering = orderingForK(k)

    //Theoretical max #k-mers in a perfect super-mer is (2 * k - m).
    //On average a super-mer is about 10 k-mers in cases we have seen.
    val estimatedSize = segments.size * 15
    val buf = new mutable.ArrayBuffer[Array[Long]](estimatedSize)

    for {
      s <- segments
      km <- s.kmersAsLongArrays(k, forwardOnly)
    } {
      buf += km
    }
    val byKmer = buf.toArray
    java.util.Arrays.sort(byKmer, ordering)

    new Iterator[(Array[Long], Abundance)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.length

      def hasNext = i < len

      def next: (Array[Long], Abundance) = {
        val lastKmer = byKmer(i)
        var count = 0L
        while (i < len && ordering.compare(byKmer(i), lastKmer) == 0) {
          count += 1
          i += 1
        }

        (lastKmer, count)
      }
    }
  }
}