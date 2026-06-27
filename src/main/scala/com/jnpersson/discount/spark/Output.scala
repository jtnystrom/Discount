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

import com.jnpersson.discount._
import com.jnpersson.discount.bucket.BucketStats
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * Output format helper methods
 */
object Output {
  /**
   * Write a data table as TSV to the filesystem.
   * @param data data to write
   * @param writeLocation location to write (prefix name, a suffix will be appended)
   */
  def writeTSV[A](data: Dataset[A], writeLocation: String): Unit =
    data.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")

  /**
   * Write k-mers with counts as FASTA files. Each k-mer becomes a separate sequence.
   * The counts are output as sequence ID headers.
   * @param allKmers data to write
   * @param writeLocation location to write (prefix name, a suffix will be appended)
   */
  def writeFastaCounts(allKmers: Dataset[(NTSeq, Abundance)], writeLocation: String)(implicit spark: SparkSession):
    Unit = {

    import spark.sqlContext.implicits._
    //There is no way to force overwrite with saveAsNewAPIHadoopFile, so delete the data manually
    val outputPath = s"${writeLocation}_counts"
    val hadoopPath = new HPath(outputPath)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(hadoopPath)) {
      println(s"Deleting pre-existing output path $outputPath")
      fs.delete(hadoopPath, true)
    }

    allKmers.map(x => (x._2.toString, x._1)).rdd.saveAsNewAPIHadoopFile(outputPath,
      classOf[String], classOf[NTSeq], classOf[FastaOutputFormat[String, NTSeq]])
  }

  /**
   * Print overview statistics for a collection of BucketStats objects.
   * @param stats Statistics to aggregate
   * @param fileOutput Location to also write output file to (optional, prefix name)
   */
  def showStats(stats: Dataset[BucketStats], fileOutput: Option[String] = None)(implicit spark: SparkSession): Unit = {
    import spark.sqlContext.implicits._

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
        (sum("totalAbundance") / sum("superKmers"), "Mean superkmer length")
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