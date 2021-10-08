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

package com.jnpersson.discount.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import com.jnpersson.discount._
import com.jnpersson.discount.hash.{InputFragment, MinSplitter, MotifSpace, Orderings}

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    //SparkConf can be customized here if needed
    new SparkConf
  }

  implicit lazy val spark = {
    val sp = SparkSession.builder().appName(appName).
      enableHiveSupport().
      master("spark://localhost:7077").config(conf).getOrCreate()

  /* Reduce the verbose INFO logs that we get by default (to some degree, edit spark's conf/log4j.properties
   * for greater control)
   */
    sp.sparkContext.setLogLevel("WARN")
    sp
  }
}

/**
 * Configuration for a Spark-based tool.
 * @param args
 * @param spark
 */
abstract class SparkToolConf(args: Array[String])(implicit spark: SparkSession) extends Configuration(args) {
  def sampling = new Sampling

  lazy val discount =
    new Discount(k(), minimizers.toOption, minimizerWidth(), ordering(), sample(),
      maxSequenceLength(), single(), normalize())
}

class DiscountConf(args: Array[String])(implicit spark: SparkSession) extends SparkToolConf(args) {
  version(s"Discount ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2021 Johan Nyström-Persson")
  banner("Usage:")

  val inFiles = trailArg[List[String]](descr = "Input sequence files", required = false)
  val min = opt[Long](descr = "Filter for minimum k-mer abundance", noshort = true)
  val max = opt[Long](descr = "Filter for maximum k-mer abundance", noshort = true)

  val presample = new RunnableCommand("sample") {
    banner("Sample m-mers to generate a minimizer ordering")
    val output = trailArg[String](required = true, descr = "Location to write the sampled ordering at")

    validate(ordering, inFiles) { (o, ifs) =>
      if (o != "frequency") Left("Sampling requires the frequency ordering (-o frequency)")
      else if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run(): Unit =
      discount.kmers(inFiles()).sample(output())
  }
  addSubcommand(presample)

  val count = new RunnableCommand("count") {
    banner("Count k-mers.")
    val output = opt[String](descr = "Location where outputs are written", required = true)

    val tsv = opt[Boolean](default = Some(false),
      descr = "Use TSV output format instead of FASTA, which is the default")

    val sequence = toggle(default = Some(true),
      descrYes = "Output sequence for each k-mer in the counts table (default true)")
    val superkmers = opt[Boolean](default = Some(false),
      descr = "Instead of k-mers, output human-readable superkmers in the counts table")
    val histogram = opt[Boolean](default = Some(false),
      descr = "Output a histogram instead of a counts table")
    val buckets = opt[Boolean](default = Some(false),
      descr = "Instead of k-mer counts, output per-bucket summaries (for minimizer testing)")

    validate(tsv, histogram, sequence, inFiles) { (t, h, s, ifs) =>
      if (h && !t) Left("Histogram output requires TSV format (--tsv)")
      else if (!s && !t) Left("FASTA output requires --sequence")
      else if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run() {
      val groupedSegments = discount.kmers(inFiles()).segments
      val counting = groupedSegments.counting(min.toOption, max.toOption)

      if (superkmers()) {
        groupedSegments.writeSupermerStrings(output())
      } else if (buckets()) {
        counting.writeBucketStats(output())
      } else if (histogram()) {
        counting.counts.writeHistogram(output())
      } else if (tsv()) {
        counting.counts.writeTSV(sequence(), output())
      } else {
        counting.counts.writeFasta(output())
      }
    }
  }
  addSubcommand(count)

  val stats = new RunnableCommand("stats") {
    banner("Show statistical summary of the dataset.")

    validate(inFiles) { ifs =>
      if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run(): Unit = {
      val kmers = discount.kmers(inFiles())
      kmers.showStats(min.toOption, max.toOption)
    }
  }
  addSubcommand(stats)
}

/**
 * Main API entry point for Discount.
 * Also see the command line examples in the documentation for more information on these options.
 *
 * @param k                 k-mer length
 * @param minimizers        location of universal k-mer hitting set (or a directory with multiple sets)
 * @param m                 minimizer width
 * @param ordering          minimizer ordering (frequency/lexicographic/given/random/signature)
 * @param sample            sample fraction for frequency orderings
 * @param normalize         whether to normalize k-mer orientation during counting. Causes every sequence to be scanned
 *                          in both forward and reverse, after which only forward orientation k-mers are kept.
 * @param singleSequence    whether to read a single long sequence, instead of multiple sequences
 * @param maxSequenceLength max length of a single sequence (short reads)
 *                          (suggested value: total number of CPUs on workers)
 * @param spark
 */
case class Discount(k: Int, minimizers: Option[String], m: Int = 10, ordering: String = "frequency",
                    sample: Double = 0.01, maxSequenceLength: Int = 1000, singleSequence: Boolean = false,
                    normalize: Boolean = false
                   )(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  private def sampling = new Sampling
  private lazy val templateSpace = MotifSpace.ofLength(m, false)

  //Validate configuration
  if (m >= k) {
    throw new Exception("m must be < k")
  }
  if (m > 15) {
    throw new Exception("m > 15 is not supported yet")
  }
  if (normalize && k % 2 == 0) {
    throw new Exception(s"normalizing mode is only supported for odd values of k (you supplied $k)")
  }

  /** Obtain an InputReader configured with settings from this object.
    */
  def inputReader = new InputReader(maxSequenceLength, k)

  /** Load reads/sequences from files according to the settings in this object.
   *
   * @param input  One or several supported files, separated by comma or space
   * @param sample Sample fraction, if any
   * @return
   */
  def getInputSequences(input: String, sample: Option[Double] = None): Dataset[NTSeq] =
    getInputFragments(input, sample).map(_.nucleotides)

  def getInputFragments(input: String, sample: Option[Double] = None): Dataset[InputFragment] = {
    val addRCReads = normalize
    inputReader.getInputFragments(input, addRCReads, sample, singleSequence)
  }

  def sequenceTitles(input: String): Dataset[SeqTitle] =
    inputReader.getSequenceTitles(input, singleSequence)

  private def getFrequencySpace(inFiles: String, validMotifs: Seq[String],
                        persistHashLocation: Option[String] = None): MotifSpace = {
    val validSetTemplate = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    val input = getInputSequences(inFiles, Some(sample))
    sampling.createSampledSpace(input, validSetTemplate, persistHashLocation)
  }

  /** Construct a read splitter for the given input files based on the settings in this object.
   *
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   */
  def getSplitter(inFiles: Option[String], persistHash: Option[String] = None): MinSplitter = {
    val template = templateSpace
    val validMotifs = (minimizers match {
      case Some(ml) =>
        val use = sampling.readMotifList(ml, k, m)
        println(s"${use.size}/${template.byPriority.size} motifs will be used (loaded from $ml)")
        use
      case None =>
        template.byPriority
    })

    val useSpace = (ordering match {
      case "given" => MotifSpace.using(validMotifs)
      case "frequency" =>
        getFrequencySpace(inFiles.getOrElse(throw new Exception("Frequency sampling can only be used with input data")),
          validMotifs, persistHash)
      case "lexicographic" =>
        //template is lexicographically ordered by construction
        MotifSpace.fromTemplateWithValidSet(template, validMotifs)
      case "random" =>
        Orderings.randomOrdering(
          MotifSpace.fromTemplateWithValidSet(template, validMotifs)
        )
      case "signature" =>
        //Signature lexicographic
        Orderings.minimizerSignatureSpace(template)
      case "signatureFrequency" =>
        val frequencyTemplate = getFrequencySpace(
          inFiles.getOrElse(throw new Exception("Frequency sampling can only be used with input data")),
          template.byPriority, persistHash)
        Orderings.minimizerSignatureSpace(frequencyTemplate)
    })
    MinSplitter(useSpace, k)
  }

  /** Load k-mers from the given files. */
  def kmers(inFiles: List[String]): Kmers =
    new Kmers(this, inFiles)

  /** Load k-mers from the given file. */
  def kmers(inFile: String): Kmers =
    kmers(List(inFile))

}

/**
 * Convenience methods for interacting with k-mers from a set of input files.
 * @param discount The Discount object
 * @param inFiles Input files
 * @param spark
 */
class Kmers(val discount: Discount, val inFiles: List[String])(implicit spark: SparkSession) {
  private def inData = inFiles.mkString(",")

  /** The read splitter associated with this set of inputs. */
  lazy val spl = discount.getSplitter(Some(inData))

  /** Broadcast of the read splitter associated with this set of inputs. */
  lazy val bcSplit = spark.sparkContext.broadcast(spl)

  /** Input fragments associated with these inputs. */
  def inputFragments: Dataset[InputFragment] =
    discount.getInputFragments(inData)

  def sequenceTitles: Dataset[SeqTitle] =
    discount.sequenceTitles(inData)

  /** Grouped segments generated from the input, which enable further processing, such as k-mer counting.
   */
  val segments: GroupedSegments =
    GroupedSegments.fromReads(discount.getInputSequences(inData), bcSplit)

  /** Convenience method to obtain a counting object for these k-mers. K-mer orientations will be normalized
   * if the Discount object was configured for this.
   *
   * @param min Lower bound for counting
   * @param max Upper bound for counting
   */
  def counting(min: Option[Long] = None, max: Option[Long] = None): GroupedSegments#Counting =
    segments.counting(min, max, discount.normalize)

  /** Cache the segments. */
  def cache(): this.type = { segments.cache(); this }

  /** Unpersist the segments. */
  def unpersist(): this.type = { segments.unpersist(); this }

  /** Sample the input data, writing the generated frequency ordering to HDFS.
   * @param writeLocation Location to write the frequency ordering to
   */
  def sample(writeLocation: String): MinSplitter =
    discount.getSplitter(Some(inData), Some(writeLocation))

  /** Convenience method to show stats for this dataset.
   * @param min Lower bound for counting
   * @param max Upper bound for counting
   */
  def showStats(min: Option[Abundance] = None, max: Option[Abundance] = None) =
    Counting.showStats(counting(min, max).bucketStats)
}

object Discount extends SparkTool("Discount") {
  def main(args: Array[String]) {
    Commands.run(new DiscountConf(args)(spark))
  }
}