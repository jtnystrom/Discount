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

import discount.bucket.BucketStats
import discount.hash.{MotifCountingScanner, _}
import discount.util.{NTBitArray, ZeroNTBitArray}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

final case class HashSegment(hash: BucketId, segment: ZeroNTBitArray)
final case class CountedHashSegment(hash: BucketId, segment: ZeroNTBitArray, count: Long)

class Routines(val spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import spark.sqlContext.implicits._

  /**
   * Count motifs such as AC, AT, TTT in a set of reads using a simple in-memory counter.
   * For reducePartitions, ideally the total number of CPUs expected to be available
   * should be passed to improve performance.
   */
  def countFeatures(reads: Dataset[String], space: MotifSpace,
                    reducePartitions: Int): MotifCounter = {
    val brScanner = sc.broadcast(new MotifCountingScanner(space))

    val r = reads.mapPartitions(rs => {
      val s = brScanner.value
      val c = MotifCounter(s.space)
      s.scanGroup(c, rs)
      Iterator(c)
    })
    r.coalesce(reducePartitions).reduce(_ + _)
  }

  /**
   * Create a MotifSpace based on sampling reads.
   * @param input Input reads
   * @param template Template space
   * @param samplePartitions The number of CPUs expected to be available for sampling
   * @param persistLocation Location to optionally write the new space to for later reuse
   * @return
   */
  def createSampledSpace(input: Dataset[String], template: MotifSpace, samplePartitions: Int,
                         persistLocation: Option[String] = None): MotifSpace = {

    val counter = countFeatures(input, template, samplePartitions)
    counter.print(template, s"Discovered frequencies in sample")

    for (loc <- persistLocation) {
      val data = sc.parallelize(counter.motifsWithCounts(template), 100).toDS()
      data.write.mode(SaveMode.Overwrite).csv(s"${loc}_hash")
    }
    counter.toSpaceByFrequency(template)
  }

  def readMotifList(location: String): Array[String] = {
    spark.read.csv(location).collect().map(_.getString(0))
  }


  /**
   * Restore persisted motif priorities. The template space must contain
   * (a subset of) the same motifs, but need not be in the same order.
   */
  def restoreSpace(location: String): MotifSpace = {
    val raw = spark.read.csv(s"${location}_hash").map(x =>
      (x.getString(0), x.getString(1).toInt)).collect
    println(s"Restored previously saved hash parameters with ${raw.size} motifs")
    MotifCounter.toSpaceByFrequency(raw)
  }
}

/**
 * Serialization-safe routines.
 */
object SerialRoutines {
  /**
   * Convenience method
   */
  def getReadsFromFiles(fileSpec: String, k: Int, withRC: Boolean = false, maxReadLength: Int = 1000,
                        sampleFraction: Option[Double] = None,
                        multilineFasta: Boolean = false)(implicit spark: SparkSession): Dataset[String] = {
    val hrf = new HadoopReadFiles(spark, maxReadLength, k, multilineFasta)
    hrf.getReadsFromFiles(fileSpec, withRC, sampleFraction)
  }

  /**
   * Convenience method
   */
  def createSampledSpace(sampledInput: Dataset[String], m: Int, samplePartitions: Int,
                         validMotifFile: Option[String])(implicit spark: SparkSession): MotifSpace = {
    val r = new Routines(spark)
    val template = MotifSpace.ofLength(m)
    val template2 = validMotifFile match {
      case Some(mf) =>
        val uhs = r.readMotifList(mf)
        MotifSpace.fromTemplateWithValidSet(template, uhs)
      case _ => template
    }
    r.createSampledSpace(sampledInput, template2, samplePartitions, None)
  }

  /**
   * Convenience method
   */
  def hashSegments[H](input: Dataset[String], spl: Broadcast[ReadSplitter[H]])
                               (implicit spark: SparkSession): Dataset[HashSegment] = {
    import spark.sqlContext.implicits._
    input.flatMap(r => createHashSegments(r, spl))
  }

  def createGroupedSegments[H](input: Dataset[String], spl: Broadcast[ReadSplitter[H]])(implicit spark: SparkSession):
    Dataset[(BucketId, Array[ZeroNTBitArray])] =
    segmentsByHash(hashSegments(input, spl))


  def segmentsByHash[H](segments: Dataset[HashSegment])(implicit spark: SparkSession):
    Dataset[(BucketId, Array[ZeroNTBitArray])] = {
    import spark.sqlContext.implicits._
    val grouped = segments.groupBy($"hash")
    grouped.agg(collect_list($"segment")).as[(BucketId, Array[ZeroNTBitArray])]
  }

  def createHashSegments[H](r: String, spl: Broadcast[ReadSplitter[H]]): Iterator[HashSegment] = {
    val splitter = spl.value
    createHashSegments(r, splitter)
  }

  def createHashSegments[H](r: String, splitter: ReadSplitter[H]): Iterator[HashSegment] = {
    for {
      (h, s) <- splitter.split(r)
      r = HashSegment(splitter.compact(h), NTBitArray.encode(s))
    } yield r
  }

  def showStats(stats: Dataset[BucketStats]): Unit = {
    def fmt(x: Any): String = {
      x match {
        case d: Double => "%.3f".format(d)
        case null => "N/A"
        case _ => x.toString
      }
    }

    val cols = Seq("distinctKmers", "totalAbundance", "superKmers")
    val aggCols = Array(sum("distinctKmers"), sum("uniqueKmers"),
      sum("totalAbundance"), sum("superKmers"),
      max("maxAbundance")) ++
      cols.flatMap(c => Seq(mean(c), min(c), max(c), stddev(c)))

    val statsAgg = stats.agg(count("superKmers"), aggCols :_*).take(1)(0)
    val allValues = (0 until statsAgg.length).map(i => fmt(statsAgg.get(i)))

    val colfmt = "%-20s %s"
    println(colfmt.format("number of buckets", allValues(0)))
    println(colfmt.format("distinct k-mers", allValues(1)))
    println(colfmt.format("unique k-mers", allValues(2)))
    println(colfmt.format("total abundance", allValues(3)))
    println(colfmt.format("superkmer count", allValues(4)))
    println(colfmt.format("max abundance", allValues(5)))
    println("Per bucket stats:")

    println(colfmt.format("", "Mean\tMin\tMax\tStd.dev"))
    for {
      (col: String, values: Seq[String]) <- (Seq("k-mers", "abundance", "superkmers").iterator zip
        allValues.drop(6).grouped(4))
    } {
      println(colfmt.format(col, values.mkString("\t")))
    }
  }
}
