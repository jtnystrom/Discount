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

package com.jnpersson.discount.bucket

import com.jnpersson.kmers._
import com.jnpersson.kmers.HDFSUtil
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{count, max, mean, min, stddev, sum}

/**
 * Statistics for a single bin/bucket.
 * @param id Minimizer/bucket ID (human-readable)
 * @param superKmers Total number of superkmers
 * @param totalAbundance Total number of k-mers counted
 * @param distinctKmers Total number of distinct k-mers
 * @param uniqueKmers Total number of k-mers with abundance == 1
 * @param maxAbundance Greatest abundance seen for a single k-mer
 */
final case class BucketStats(id: String, superKmers: Long, totalAbundance: Abundance, distinctKmers: Long,
                             uniqueKmers: Long, maxAbundance: Abundance) {
  def merge(other: BucketStats): BucketStats = {
    BucketStats(id, superKmers + other.superKmers,
      totalAbundance + other.totalAbundance,
      distinctKmers + other.distinctKmers,
      uniqueKmers + other.uniqueKmers,
      if (maxAbundance > other.maxAbundance) maxAbundance else other.maxAbundance
    )
  }

  /** Test whether k-mer counts are equivalent, ignoring minimizer ordering effects (bucket ID and superkmer count) */
  def equalCounts(other: BucketStats): Boolean = {
    totalAbundance == other.totalAbundance &&
      distinctKmers == other.distinctKmers &&
      uniqueKmers == other.uniqueKmers &&
      maxAbundance == other.maxAbundance
  }
}

object BucketStats {

  def empty =
    BucketStats("", 0, 0, 0, 0, 0)

  /**
   * Collect all statistics into a new BucketStats object
   * @param id Human-readable ID of the bucket
   * @param counts Counts for each k-mer in the bucket (grouped by super-mer)
   * @return Aggregate statistics for the bucket
   */
  def collectFromCounts(id: String, counts: Array[Array[Int]]): BucketStats = {
    var totalAbundance: Abundance = 0
    var distinctKmers: Abundance = 0
    var uniqueKmers: Abundance = 0
    var maxAbundance: Abundance = 0
    val superKmers = counts.length

    var i = 0
    while (i < counts.length) {
      var j = 0
      val row = counts(i)
      while (j < row.length) {
        val item = row(j)
        if (item != 0) {
          totalAbundance += item
          distinctKmers += 1
          if (item == 1) { uniqueKmers += 1 }
          if (item > maxAbundance) { maxAbundance = item }
        }
        j += 1
      }
      i += 1
    }
    BucketStats(id, superKmers, totalAbundance, distinctKmers, uniqueKmers, maxAbundance)
  }

  /**
   * Print overview statistics for a collection of BucketStats objects.
   * @param stats Statistics to aggregate
   * @param fileOutput Location to also write output file to (optional, prefix name)
   */
  def show(stats: Dataset[BucketStats], fileOutput: Option[String] = None)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    def formatWideNumber(x: Any): String = {
      x match {
        case d: Double => "%18.3f".format(d)
        case l: Long => "%,18d".format(l)
        case null => "N/A"
        case _ => x.toString
      }
    }

    def formatNumber(x: Any): String = {
      x match {
        case d: Double => "%.3f".format(d)
        case null => "N/A"
        case _ => x.toString
      }
    }

    val writer = fileOutput.map(o => HDFSUtil.getPrintWriter(o + "_stats.txt"))

    def printBoth(s: String): Unit = {
      println(s)
      for (w <- writer) w.println(s)
    }

    try {
      //(spark column, label)
      val baseColumns = Array(
        ("distinctKmers", "k-mers"),
        ("totalAbundance", "abundance"),
        ("superKmers", "superkmers"))
      val wideFormatColumns = Array(
        (count("superKmers"), "Number of buckets"),
        (sum("distinctKmers"), "Distinct k-mers"),
        (sum("uniqueKmers"), "Unique k-mers"),
        (sum("totalAbundance"), "Total abundance"),
        (sum("totalAbundance") / sum("distinctKmers"), "Mean abundance"),
        (max("maxAbundance"), "Max abundance"),
        (sum("superKmers"), "Superkmer count"),
        (sum("distinctKmers") / sum("superKmers"), "Mean superkmer length")
      )

      //Aggregate all of the base columns in four ways
      val aggregateColumns =
        baseColumns.map(_._1).flatMap(c => List(mean(c), min(c), max(c), stddev(c))) ++
          wideFormatColumns.map(_._1)

      //Calculate all the aggregate columns in one query
      val statsAgg = stats.filter($"totalAbundance" > 0).
        agg(aggregateColumns.head, aggregateColumns.tail :_*).first().
        toSeq
      val baseAggregations = baseColumns.length * 4
      val baseOutput = statsAgg.take(baseAggregations).map(formatNumber)
      val wideOutput = statsAgg.drop(baseAggregations).map(formatWideNumber)

      val colfmt = "%-25s %s"
      printBoth("==== Overall statistics ====")
      for { ((_, label), str) <- wideFormatColumns zip wideOutput} {
        printBoth(colfmt.format(label, str))
      }

      printBoth("==== Per bucket (minimizer) statistics ====")
      val fourColFormat = "%14s%10s%10s%14s"
      printBoth(colfmt.format("", fourColFormat.format("Mean", "Min", "Max", "Std.dev")))
      for { (col, values) <- baseColumns.map(_._2).iterator zip baseOutput.grouped(4) } {
        printBoth(colfmt.format(col, fourColFormat.format(values: _*)))
      }
    } finally {
      for { w <- writer } w.close()
    }
  }

}