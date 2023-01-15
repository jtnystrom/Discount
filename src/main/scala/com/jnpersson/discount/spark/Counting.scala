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
 * Serialization-safe methods for counting
 */
object Counting {
  /**
   * Write a data table as TSV to the filesystem.
   * @param allKmers data to write
   * @param writeLocation location to write (prefix name, a suffix will be appended)
   */
  def writeTSV[A](allKmers: Dataset[A], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")
  }


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

    def longFmt(x: Any): String = {
      x match {
        case d: Double => "%18.3f".format(d)
        case l: Long => "%,18d".format(l)
        case null => "N/A"
        case _ => x.toString
      }
    }

    def shortFmt(x: Any): String = {
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
      val baseColumns = List("distinctKmers", "totalAbundance", "superKmers")
      val aggregateColumns = Array(sum("distinctKmers"), sum("uniqueKmers"),
        sum("totalAbundance"),
        sum("totalAbundance") / sum("distinctKmers"),
        max("maxAbundance"), sum("superKmers"),
        sum("totalAbundance") / sum("superKmers")) ++
        baseColumns.flatMap(c => List(mean(c), min(c), max(c), stddev(c)))

      val statsAgg = stats.filter($"totalAbundance" > 0).
        agg(count("superKmers"), aggregateColumns: _*).take(1)(0)
      val longFormat = statsAgg.toSeq.take(8).map(longFmt)
      val shortFormat = statsAgg.toSeq.drop(8).map(shortFmt)

      val colfmt = "%-25s %s"
      printBoth("==== Overall statistics ====")
      printBoth(colfmt.format("Number of buckets", longFormat(0)))
      printBoth(colfmt.format("Distinct k-mers", longFormat(1)))
      printBoth(colfmt.format("Unique k-mers", longFormat(2)))
      printBoth(colfmt.format("Total abundance", longFormat(3)))
      printBoth(colfmt.format("Mean abundance", longFormat(4)))
      printBoth(colfmt.format("Max abundance", longFormat(5)))
      printBoth(colfmt.format("Superkmer count", longFormat(6)))
      printBoth(colfmt.format("Mean superkmer length", longFormat(7)))
      printBoth("==== Per bucket (minimizer) statistics ====")

      printBoth(colfmt.format("", "Mean\tMin\tMax\tStd.dev"))
      for {
        (col: String, values: Seq[String]) <- Seq("k-mers", "abundance", "superkmers").iterator zip
          shortFormat.grouped(4)
      } {
        printBoth(colfmt.format(col, values.mkString("\t")))
      }
    } finally {
      for (w <- writer) w.close()
    }
  }
}