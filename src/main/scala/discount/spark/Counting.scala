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