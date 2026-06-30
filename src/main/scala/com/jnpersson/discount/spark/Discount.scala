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
import com.jnpersson.discount.bucket.ReducibleBucket
import com.jnpersson.kmers._
import com.jnpersson.kmers.input.{FileInputs, InputReader, Ungrouped}
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.minimizer.{InputFragment, MinSplitter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Main API entry point for Discount.
 * Also see the command line examples in the documentation for more information on these options.
 *
 * @param k                 k-mer length
 * @param minimizers        source of minimizers. See [[MinimizerSource]]
 * @param m                 minimizer width
 * @param ordering          minimizer ordering. See [[MinimizerOrdering]]
 * @param sample            sample fraction for frequency orderings
 * @param maxSequenceLength max length of a single sequence (for short reads)
 * @param normalize         whether to normalize k-mer orientation during counting. Causes every sequence to be scanned
 *                          in both forward and reverse, after which only forward orientation k-mers are kept.
 * @param method            counting method to use (or None for automatic selection). See [[CountMethod]]
 * @param partitions        number of shuffle partitions/index buckets
 * @param spark             the SparkSession
 */
final case class Discount(k: Int, minimizers: MinimizerSource = Bundled, m: Int = 10,
                          ordering: MinimizerOrdering = Frequency(), sample: Double = 0.01,
                          normalize: Boolean = false, method: CountMethod = Auto,
                          partitions: Int = 200)(implicit spark: SparkSession)
  extends MinimizerConfig(k, minimizers, m, ordering, sample) {
    import spark.implicits._

  if (normalize && k % 2 == 0) {
    throw new Exception(s"normalizing mode is only supported for odd values of k (you supplied $k)")
  }

  def orientationFilter: Orientation =
    if (normalize) ForwardOnly else Both

  /** Obtain an InputReader configured with settings from this object.
   * @param files Files to read. Can be a single file or multiple files.
   *              Wildcards can be used. A name of the format @list.txt
   *              will be parsed as a list of files.
   */
  def inputReader(files: String*) = new FileInputs(files, k, Ungrouped)

  /** Load reads/sequences from files according to the settings in this object.
   * @param files  input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputSequences(files: Seq[String], addRCReads: Boolean): Dataset[NTSeq] = {
    val fs = getInputFragments(files)
    val fs2 = if (addRCReads) InputReader.addRCFragments(fs) else fs
    fs2.map(_.nucleotides)
  }

  /** Single file version of the same method */
  def getInputSequences(file: String, addRCReads: Boolean = false): Dataset[NTSeq] =
    getInputSequences(List(file), addRCReads)

  /** Load input fragments (with sequence title and location) according to the settings in this object.
   * @param files input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputFragments(files: Seq[String]): Dataset[InputFragment] =
    inputReader(files: _*).getInputFragments()

  /** Single file version of the same method */
  def getInputFragments(file: String): Dataset[InputFragment] =
    getInputFragments(List(file))

  /** Load sequence titles only from the given input files */
  def sequenceTitles(input: String*): Dataset[SeqTitle] =
    inputReader(input :_*).getSequenceTitles


  private def newSession(buckets: Int): SparkSession = {
    val session = spark.newSession()
    //Ensure partitioning always uses the expected number of buckets for this data by creating a special session for it.
    //The main SparkContext is unchanged.
    session.conf.set("spark.sql.shuffle.partitions", buckets.toString)
    session
  }

  /** Load k-mers from the given files. */
  def kmers(inFiles: String*): Kmers =
    new Kmers(this, inFiles, None)(newSession(partitions))

  /** Load k-mers from the given files. */
  def kmers(knownSplitter: Broadcast[AnyMinSplitter], inFiles: String*): Kmers = {
    new Kmers(this, inFiles, Some(knownSplitter))(newSession(partitions))
  }

  /**
   * Convenience method to construct a counting k-mer index containing all k-mers from the input sequence files.
   * If a frequency minimizer ordering is used (which is the default), the input files will be sampled and a
   * new minimizer ordering will be constructed.
   * @param inFiles input files
   */
  def index(inFiles: String*): Index = kmers(inFiles : _*).index

  /**
   * Convenience method to construct a compatible counting k-mer index containing all k-mers from the
   * input sequence files.
   * @param compatible Compatible index to copy settings, such as an existing minimizer ordering, from
   * @param inFiles input files
   */
  def index(compatible: Index, inFiles: String*): Index = compatible.newCompatible(this, inFiles: _*)

  /** Construct an empty index, using the supplied sequence files to prepare the minimizer ordering.
   * This is useful when a frequency ordering is used and one wants to sample a large number of files in advance.
   * [[Index.newCompatible]] or index(compatible: Index, inFiles: String*)
   *  can then be used to construct compatible indexes with actual k-mers using
   * the resulting ordering.
   * @param inFiles The input files to sample for frequency orderings
   * */
  def emptyIndex(inFiles: String*): Index = {
    val splitter = new Kmers(this, inFiles, None)(newSession(partitions)).bcSplit
    new Index(IndexParams(splitter, partitions, ""), List[ReducibleBucket]().toDS())
  }
}

/**
 * Convenience methods for interacting with k-mers from a set of input files.
 *
 * @param discount The Discount object
 * @param inFiles Input files
 * @param knownSplitter The splitter/minimizer scheme to use, if one is available.
 *                      Otherwise, a new one will be constructed.
 */
class Kmers(val discount: Discount, val inFiles: Seq[String], knownSplitter: Option[Broadcast[AnyMinSplitter]] = None)
           (implicit spark: SparkSession) {

  /** Broadcast of the read splitter associated with this set of inputs. */
  lazy val bcSplit: Broadcast[AnyMinSplitter] = knownSplitter.getOrElse(
    spark.sparkContext.broadcast(discount.getSplitter(Some(inFiles))))

  /** The overall method used for k-mer counting. If not specified, this will be guessed
   * from the input data according to a heuristic. */
  lazy val method: CountMethod = discount.method.resolve(bcSplit.value.priorities)

  /** Input fragments associated with these inputs. */
  def inputFragments: Dataset[InputFragment] = {
    val fs = discount.getInputFragments(inFiles)
    if (discount.normalize) InputReader.addRCFragments(fs) else fs
  }

  def sequenceTitles: Dataset[SeqTitle] =
    discount.sequenceTitles(inFiles: _*)

  /** Sample the input data, counting minimizers and writing the generated frequency ordering to HDFS.
   * @param writeLocation Location to write the frequency ordering to
   * @return A splitter object corresponding to the generated ordering
   */
  def constructSampledMinimizerOrdering(writeLocation: String): MinSplitter[_] =
    discount.getSplitter(Some(inFiles), Some(writeLocation))

  private def inputSequences = discount.getInputSequences(inFiles, method.addRCToMainData(discount))

  def segments: GroupedSegments =
    GroupedSegments.fromReads(inputSequences, method, discount.normalize, bcSplit)

  private def makeIndex(input: Dataset[NTSeq]): Index =
    GroupedSegments.fromReads(input, method, discount.normalize, bcSplit).
      toIndex(discount.orientationFilter, discount.partitions)

  /** A counting k-mer index containing all k-mers from the input sequences. */
  lazy val index: Index = makeIndex(inputSequences)

  /** Construct an index from a sampled fraction of this input data. Because repeated calls will
   * sample the input differently, it is recommended to cache the Index if it will be used repeatedly.
   */
  def sampledIndex(fraction: Double): Index =
    makeIndex(inputSequences.sample(fraction))
}

/** Main command-line interface to Discount. */
object Discount extends SparkTool("Discount") {
  def main(args: Array[String]): Unit = {
    try {
      val spark = sparkSession()
      Commands.run(new DiscountConf(args.toSeq)(spark).finishSetup())
    } catch {
      case se: ScallopExitException =>
        handleScallopException(se)
    }
  }
}