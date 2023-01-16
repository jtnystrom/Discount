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

import com.jnpersson.discount
import org.apache.spark.sql.{Dataset, SparkSession}
import com.jnpersson.discount._
import com.jnpersson.discount.bucket.ReducibleBucket
import com.jnpersson.discount.hash._
import org.apache.spark.broadcast.Broadcast

import scala.util.Random

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
                          ordering: MinimizerOrdering = Frequency, sample: Double = 0.01, maxSequenceLength: Int = 1000000,
                          normalize: Boolean = false, method: CountMethod = Auto,
                          partitions: Int = 200)(implicit spark: SparkSession)  {
    import spark.sqlContext.implicits._

  private def sampling = new Sampling
  private lazy val templateTable = MinTable.ofLength(m)

  //Validate configuration
  if (m >= k) {
    throw new Exception("m must be < k")
  }
  if (m > 32) {
    throw new Exception("m > 32 is not supported yet")
  }
  if (normalize && k % 2 == 0) {
    throw new Exception(s"normalizing mode is only supported for odd values of k (you supplied $k)")
  }

  /** Obtain an InputReader configured with settings from this object.
   * @param files Files to read. Can be a single file or multiple files.
   *              Wildcards can be used. A name of the format @list.txt
   *              will be parsed as a list of files.
   */
  def inputReader(files: String*) = new Inputs(files, k, maxSequenceLength)

  /** Load reads/sequences from files according to the settings in this object.
   * @param files  input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputSequences(files: Seq[String], addRCReads: Boolean): Dataset[NTSeq] =
    getInputFragments(files, addRCReads).map(_.nucleotides)

  /** Single file version of the same method */
  def getInputSequences(file: String, addRCReads: Boolean = false): Dataset[NTSeq] =
    getInputSequences(List(file), addRCReads)

  /** Load input fragments (with sequence title and location) according to the settings in this object.
   * @param files input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputFragments(files: Seq[String], addRCReads: Boolean): Dataset[InputFragment] =
    inputReader(files: _*).getInputFragments(addRCReads)

  /** Single file version of the same method */
  def getInputFragments(file: String, addRCReads: Boolean = false): Dataset[InputFragment] =
    getInputFragments(List(file), addRCReads)

  /** Load sequence titles only from the given input files */
  def sequenceTitles(input: String*): Dataset[SeqTitle] =
    inputReader(input :_*).getSequenceTitles

  /** Efficient frequency MinTable construction method.
   * The ordering of validMotifs will be preserved in the case of equally frequent motifs.
   * @param inFiles files to sample
   * @param validMotifs valid minimizers to keep (others will be ignored)
   * @param persistHashLocation location to persist the generated minimizer ordering, if any
   * @return A frequency-based MinTable
   */
  private def getFrequencyTable(inFiles: List[String], validMotifs: Seq[NTSeq],
                                persistHashLocation: Option[String] = None): MinTable = {
    val validSetTemplate = MinTable.using(validMotifs)

    //Keep ambiguous bases for efficiency - avoids a regex split
    val input = inputReader(inFiles: _*).getInputFragments(normalize, withAmbiguous = true).
      map(_.nucleotides)
    sampling.createSampledTable(input, validSetTemplate, sample, persistHashLocation)
  }

  /** Construct a read splitter for the given input files based on the settings in this object.
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   * @return a MinSplitter configured with a minimizer ordering and corresponding MinTable
   */
  def getSplitter(inFiles: Option[Seq[String]], persistHash: Option[String] = None):
    MinSplitter[_ <: MinimizerPriorities] = {

    (minimizers, ordering) match {
      case (All, discount.Random) =>
        val seed = Random.nextLong()
        println(s"Using RandomXOR with seed $seed")
        return MinSplitter(RandomXOR(m, Random.nextLong(), canonical = false), k)
      case _ =>
    }

    if (m > 15) {
      throw new Exception("The requested minimizer ordering can only be used with m <= 15.")
    }

    lazy val validMotifs = minimizers.load(k, m)

    val useTable = ordering match {
      case Given => MinTable.using(validMotifs)
      case Frequency =>
        getFrequencyTable(inFiles.getOrElse(List()).toList, validMotifs, persistHash)
      case Lexicographic =>
        //template is lexicographically ordered by construction
        MinTable.filteredOrdering(templateTable, validMotifs)
      case discount.Random =>
        Orderings.randomOrdering(
          MinTable.filteredOrdering(templateTable, validMotifs)
        )
      case Signature =>
        Orderings.minimizerSignatureTable(templateTable)
    }

    minimizers.finish(useTable, k)
  }

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
   * @param buckets Number of index buckets to use with Spark - for moderately sized indexes, 200 is usually fine
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
 * @param spark
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
  def inputFragments: Dataset[InputFragment] =
    discount.getInputFragments(inFiles, discount.normalize)

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
      toIndex(discount.normalize, discount.partitions)

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
    val conf = new DiscountConf(args)
    Commands.run(conf)(sparkSession(conf))
  }
}
