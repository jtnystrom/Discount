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
import discount.bucket.BucketStats
import org.apache.hadoop.fs.Path
import discount.util.{KmerTable, NTBitArray}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


/**
 * Min/max abundance filtering for k-mer counts
 * @param min
 * @param max
 */
final case class CountFilter(min: Option[Abundance], max: Option[Abundance]) {
  val active = min.nonEmpty || max.nonEmpty
  val minValue = min.getOrElse(Long.MinValue)
  val maxValue = max.getOrElse(Long.MaxValue)

  def filter(x: (Array[Long], Abundance)): Boolean = {
    !active || (x._2 >= minValue && x._2 <= maxValue)
  }
}

/**
 * Serialization-safe methods for counting
 */
object Counting {

  /**
   * From a series of sequences (where k-mers may be repeated),
   * produce an iterator with counted abundances where each k-mer appears only once.
   * @param segments
   * @param k
   * @return
   */
  def countsFromSequences(segments: Iterable[NTBitArray], k: Int,
                          forwardOnly: Boolean): Iterator[(Array[Long], Abundance)] =
    KmerTable.fromSegments(segments, k, forwardOnly).countedKmers


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
  def writeFastaCounts(allKmers: Dataset[(NTSeq, Long)], writeLocation: String)(implicit spark: SparkSession):
    Unit = {

    import spark.sqlContext.implicits._
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

  /**
   * Print overview statistics for a collection of BucketStats objects.
   * @param stats
   */
  def showStats(stats: Dataset[BucketStats]): Unit = {
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

    val baseColumns = List("distinctKmers", "totalAbundance", "superKmers")
    val aggregateColumns = Array(sum("distinctKmers"), sum("uniqueKmers"),
      sum("totalAbundance"),
      sum("totalAbundance") / sum("distinctKmers"),
      sum("superKmers"), max("maxAbundance")) ++
      baseColumns.flatMap(c => List(mean(c), min(c), max(c), stddev(c)))

    val statsAgg = stats.agg(count("superKmers"), aggregateColumns :_*).take(1)(0)
    val longFormat = statsAgg.toSeq.take(7).map(longFmt)
    val shortFormat = statsAgg.toSeq.drop(7).map(shortFmt)

    val colfmt = "%-20s %s"
    println("==== Overall statistics ====")
    println(colfmt.format("Number of buckets", longFormat(0)))
    println(colfmt.format("Distinct k-mers", longFormat(1)))
    println(colfmt.format("Unique k-mers", longFormat(2)))
    println(colfmt.format("Total abundance", longFormat(3)))
    println(colfmt.format("Mean abundance", longFormat(4)))
    println(colfmt.format("Max abundance", longFormat(6)))
    println(colfmt.format("Superkmer count", longFormat(5)))
    println("==== Per bucket/partition statistics ====")

    println(colfmt.format("", "Mean\tMin\tMax\tStd.dev"))
    for {
      (col: String, values: Seq[String]) <- (Seq("k-mers", "abundance", "superkmers").iterator zip
        shortFormat.grouped(4))
    } {
      println(colfmt.format(col, values.mkString("\t")))
    }
  }
}