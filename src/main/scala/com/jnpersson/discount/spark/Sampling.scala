/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
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

import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.hash._
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Routines for creating and managing frequency sampled minimizer orderings.
 * @param spark
 */
class Sampling(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  /**
   * Count motifs (m-length minimizers) in a set of reads.
   */
  def motifCounts(reads: Dataset[NTSeq], space: MotifSpace, partitions: Int): Array[(Long, Int)] = {
    //Coalescing to a specified number of partitions is useful when sampling a huge dataset,
    //where the partition number may need to be large later on in the pipeline, but for efficiency,
    //needs to be much smaller at this stage.
    val minPartitions = 200
    val coalPart = if (partitions > minPartitions) partitions else minPartitions

    val scan = spark.sparkContext.broadcast(space.scanner)
    reads.mapPartitions(it => {
      val scanner = scan.value
      it.flatMap(read => scanner.allMatches(read)._2)
    }).toDF("motif").where($"motif" =!= -1).
      coalesce(coalPart).
      groupBy("motif").agg(count("motif").as("count")).
      select($"motif", $"count".cast("int")).as[(Long, Int)].
      collect()
  }

  def countFeatures(reads: Dataset[NTSeq], space: MotifSpace, partitions: Int): SampledFrequencies = {
    SampledFrequencies(space, motifCounts(reads, space, partitions))
  }

  /**
   * Create a MotifSpace based on sampling reads.
   * @param input Input reads
   * @param template Template space, containing minimizers to sort according to frequencies in the sample
   * @param sampledFraction Fraction of input data to sample
   * @param persistLocation Location to optionally write the new space to for later reuse
   * @return
   */
  def createSampledSpace(input: Dataset[NTSeq], template: MotifSpace,
                         sampledFraction: Double,
                         persistLocation: Option[String] = None): MotifSpace = {

    val partitions = (input.rdd.getNumPartitions * sampledFraction).toInt
    val frequencies = countFeatures(input, template, partitions)
    println("Discovered frequencies in sample")
    frequencies.print()

    val r = frequencies.toSpace(sampledFraction)
    persistLocation match {
      case Some(loc) =>
        /**
         * Writes two columns: minimizer, count.
         * We write the second column (counts) for informative purposes only.
         * It will not be read back into the application later when the minimizer ordering is reused.
         */
        val raw = frequencies.motifsWithCounts
        val persistLoc = s"${loc}_minimizers_sample.txt"
        Util.writeTextFile(persistLoc, raw.map(x => x._1 + "," + x._2).mkString("", "\n", "\n"))
        println(s"Saved ${r.byPriority.length} minimizers and sampled counts to $persistLoc")
      case _ =>
    }
    r
  }

  /**
   * Write a splitter's minimizer ordering to a file
   * @param splitter Splitter containing the ordering to write
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def persistMinimizers(splitter: MinSplitter[_], location: String): Unit = {
    splitter.priorities match {
      case ms: MotifSpace =>
        persistMinimizers(ms, location)
      case _ =>
        println("Not persisting minimizer ordering (not a MotifSpace)")
    }
  }

  /**
   * Write a MotifSpace's minimizer ordering to a file
   * @param space The ordering to write
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def persistMinimizers(space: MotifSpace, location: String): Unit = {
    val persistLoc = s"${location}_minimizers.txt"
    Util.writeTextFile(persistLoc, space.byPriority.mkString("", "\n", "\n"))
    println(s"Saved ${space.byPriority.length} minimizers to $persistLoc")
  }

  /**
   * Read a saved minimizer ordering/motif list
   * @param location Location to read from. If the location is a directory, it will be scanned for files called
   *                 minimizers_{k}_{m} for various values of m and k and the most optimal file will be used.
   *                 If it is a file, the file will be read as is.
   * @param k
   * @param m
   * @return
   */
  def readMotifList(location: String, k: Int, m: Int): Array[String] = {
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

  def readMotifList(location: String): Array[String] =
    spark.read.csv(location).map(_.getString(0)).collect()
}

object Sampling {
  /**
   * Given a directory with files such as minimizers_28_10.txt,
   * minimizers_55_9.txt... (minimizers_${k}_${m}.txt), find the most
   * appropriate universal k-mer hitting set (for example, generated by PASHA) by searching all possible filenames.
   * In general, it's possible to use a universal k-mer hitting set generated for a smaller k
   * (with some loss of performance), but m must be the same.
   * See https://github.com/ekimb/pasha.
   * @param minimizerDir
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
        s"The file ${filePaths.head} (or a compatible file for a smaller k) was not found. " +
          "Generate this file (e.g. using PASHA), or run without a minimizer set."))
  }
}

