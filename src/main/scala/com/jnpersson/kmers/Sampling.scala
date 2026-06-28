/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.util.NTBitArray
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

/**
 * Routines for creating and managing frequency sampled minimizer orderings.
 */
class Sampling(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  /**
   * Count motifs (m-length minimizers) in a set of reads.
   * @param reads Data to count motifs in
   * @param table Template table with a motif set
   * @return a DataFrame with (motif, count) pairs
   */
  def motifCounts(reads: Dataset[NTSeq], table: MinTable, partitions: Int): Dataset[(Int, Int)] = {
    //Coalescing to a specified number of partitions is useful when sampling a huge dataset,
    //where the partition number may need to be large later on in the pipeline, but for efficiency,
    //needs to be much smaller at this stage.
    val minPartitions = 200
    val coalPart = if (partitions > minPartitions) partitions else minPartitions

    assert(table.width <= 15)
    val scan = spark.sparkContext.broadcast(table.scanner)
    reads.mapPartitions(it => {
      val scanner = scan.value
      it.flatMap(read => scanner.matchesOnly(read).map(_.toInt))
    }).toDF("motif").
      coalesce(coalPart).
      groupBy("motif").agg(count("motif").as("count")).
      select($"motif", $"count".cast("int")).as[(Int, Int)]
  }

  /**
   * Count motifs (m-length minimizers) in a set of sequences. The number of distinct sequences for each
   * motif will be counted.
   * @param reads Sequences to count minimizers in. Sequence title is used to identify distinct sequences,
   *              which may be split into fragments.
   * @param table Template table with a motif set
   * @return a DataFrame with (motif, count) pairs
   */
  def motifCountsBySequence(reads: Dataset[InputFragment], table: MinTable, partitions: Int): Dataset[(Int, Int)] = {
    //Coalescing to a specified number of partitions is useful when sampling a huge dataset,
    //where the partition number may need to be large later on in the pipeline, but for efficiency,
    //needs to be much smaller at this stage.
    val minPartitions = 200
    val coalPart = if (partitions > minPartitions) partitions else minPartitions

    assert(table.width <= 15)
    val scan = spark.sparkContext.broadcast(table.scanner)

    reads.mapPartitions(it => {
      val scanner = scan.value
      it.flatMap(frag => scanner.matchesOnly(frag.nucleotides).map(x => (frag.header, x.toInt)))
    }).toDF("header", "motif").
      coalesce(coalPart).
      groupBy("motif").agg(count_distinct($"header").as("count")).
      select($"motif", $"count".cast("int")).as[(Int, Int)]
  }

  def countFeatures(reads: Dataset[NTSeq], table: MinTable, partitions: Int): SampledFrequencies =
    collectFrequencies(motifCounts(reads, table, partitions), table)

  def countFeaturesBySequence(reads: Dataset[InputFragment], table: MinTable, partitions: Int): SampledFrequencies =
    collectFrequencies(motifCountsBySequence(reads, table, partitions), table)

  /** Gather sampled frequencies to the driver so that a SampledFrequencies instance can be constructed. */
  private def collectFrequencies(fs: Dataset[(Int, Int)], table: MinTable): SampledFrequencies = {
    if (table.byPriority.length > (1 << 24)) { //i.e. wider than m=12 with all minimizers
      val counts = fs.cache()
      //Materialize the dataset before getting the iterator
      counts.count()
      try {
        //This avoids collect(), which is too expensive when the number of entries is large.
        SampledFrequencies(table, counts.toLocalIterator().asScala)
      } finally {
        counts.unpersist()
      }
    } else {
      //Faster method for small datasets
      SampledFrequencies(table, fs.collect.iterator)
    }
  }


  /**
   * Create a MinTable based on sampling reads for minimizer frequencies.
   * @param input Input data (already sampled)
   * @param template Template table, containing minimizers to sort according to frequencies in the sample
   * @param sampledFraction Fraction of input data that was sampled. Note: the input data is assumed to already be
   *                        sampled, but we need this value again for data adjustments.
   * @param persistLocation Location to optionally write the new table to for later reuse
   * @param bySequence whether to count the number of distinct sequences for each motif, rather than aggregate
   *                   count
   * @return
   */
  def createSampledTable(input: Dataset[InputFragment], template: MinTable,
                         sampledFraction: Double,
                         persistLocation: Option[String] = None,
                         bySequence: Boolean = false): MinTable = {
    import spark.sqlContext.implicits._
    val partitions = (input.rdd.getNumPartitions * sampledFraction).toInt
    //Keep ambiguous bases for efficiency - avoids a regex split
    val frequencies =
      if (bySequence) {
        countFeaturesBySequence(input, template, partitions)
      } else {
        val reads = input.flatMap(x => List(Some(x.nucleotides), x.nucleotides2).flatten)
        countFeatures(reads, template, partitions)
      }
    println("Discovered frequencies in sample")
    frequencies.print()

    val r = frequencies.toTable()
    persistLocation match {
      case Some(loc) => writeFrequencies(frequencies, loc)
      case _ =>
    }
    r
  }

  def writeFrequencies(f: SampledFrequencies, location: String): Unit = {
    /**
     * Writes two columns: minimizer, count.
     * We write the second column (counts) for informative purposes only.
     * It will not be read back into the application later when the minimizer ordering is reused.
     */
    val raw = f.motifsAndCounts
    val persistLoc = s"${location}_minimizers_sample.txt"
    HDFSUtil.writeTextFile(persistLoc, raw.map(x => x._1 + "," + x._2).mkString("", "\n", "\n"))
    println(s"Saved ${raw.length} minimizers and sampled counts to $persistLoc")
  }

  /**
   * Read a saved minimizer ordering/motif list
   *
   * @param location Location to read from. If the location is a directory, it will be scanned for files called
   *                 minimizers_{k}_{m} for various values of m and k and the most optimal file will be used.
   *                 If it is a file, the file will be read as is.
   * @param k        k-mer width
   * @param m        minimizer length
   * @return
   */
  def readMotifList(location: String, k: Int, m: Int): Dataset[Int] = {
    assert(m <= 15)
    val hadoopDir = new HPath(location)
    val fs = hadoopDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.getFileStatus(hadoopDir).isDirectory) {
      println(s"$location is a directory; searching for minimizer sets")
      val selected = Sampling.findBestMinimizerFile(location, k, m)
      println(s"Using minimizers from $selected")
      readMotifList(selected)
    } else {
      readMotifList(location)
    }
  }

  def readMotifWidth(location: String): Int =
    spark.read.csv(location).as[String].map(x => x.length).collect().headOption.getOrElse(0)

  def readMotifList(location: String): Dataset[Int] =
    spark.read.csv(location).map(m => NTBitArray.encode(m.getString(0)).toInt)
}

object Sampling {
  /**
   * Given a directory with files such as minimizers_28_10.txt,
   * minimizers_55_9.txt... (minimizers_${k}_${m}.txt), find the most
   * appropriate universal k-mer hitting set (for example, generated by PASHA) by searching all possible filenames.
   * In general, it's possible to use a universal k-mer hitting set generated for a smaller k
   * (with some loss of performance), but m must be the same.
   * See https://github.com/ekimb/pasha.
   *
   * @param minimizerDir Directory to scan
   * @param k            k-mer width
   * @param m            minimizer length
   */
  def findBestMinimizerFile(minimizerDir: String, k: Int, m: Int)(implicit spark: SparkSession): String = {
    if (k <= m) {
      throw new Exception("k is less than or equal to m")
    }

    val filePaths = k.to(m + 1, -1).toList.map(k => new HPath(s"$minimizerDir/minimizers_${k}_$m.txt"))
    val hadoopDir = new HPath(minimizerDir)
    val fs = hadoopDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    filePaths.find(fs.exists).map(f => f.toUri.toString).
      getOrElse(throw new Exception(
        s"The file ${filePaths.headOption.getOrElse("")} (or a compatible file for a smaller k) was not found. " +
          "Generate this file (e.g. using PASHA), or run without a minimizer set."))
  }
}

