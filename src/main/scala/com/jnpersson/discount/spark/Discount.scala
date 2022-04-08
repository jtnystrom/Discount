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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import com.jnpersson.discount._
import com.jnpersson.discount.hash.{BundledMinimizers, InputFragment, MinSplitter, MotifSpace, Orderings}
import com.jnpersson.discount.spark.minimizers.Source
import org.apache.spark.broadcast.Broadcast

/** A tool that runs using Spark */
abstract class SparkTool(appName: String) {
  /** The Spark configuration */
  def conf: SparkConf = {
    //SparkConf can be customized here if needed
    new SparkConf
  }

  /** The SparkSession */
  implicit lazy val spark: SparkSession = {
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
    new Discount(k(), parseMinimizerSource, minimizerWidth(), ordering(), sample(), maxSequenceLength(), normalize(),
      parseMethod)
}

class DiscountConf(args: Array[String])(implicit spark: SparkSession) extends SparkToolConf(args) {
  version(s"Discount ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2021 Johan Nyström-Persson")
  banner("Usage:")

  val inFiles = trailArg[List[String]](descr = "Input sequence files", required = false)
  val min = opt[Abundance](descr = "Filter for minimum k-mer abundance", noshort = true)
  val max = opt[Abundance](descr = "Filter for maximum k-mer abundance", noshort = true)

  val presample = new RunnableCommand("sample") {
    banner("Sample m-mers to generate a minimizer ordering")
    val output = trailArg[String](required = true, descr = "Location to write the sampled ordering at")

    validate(ordering, inFiles) { (o, ifs) =>
      if (o != "frequency") Left("Sampling requires the frequency ordering (-o frequency)")
      else if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run(): Unit =
      discount.kmers(inFiles() :_*).constructSampledMinimizerOrdering(output())
  }
  addSubcommand(presample)

  val count = new RunnableCommand("count") {
    banner("Count k-mers.")
    val output = opt[String](descr = "Location where the output is written", required = true)

    val tsv = opt[Boolean](default = Some(false),
      descr = "Use TSV output format instead of FASTA, which is the default")

    val sequence = toggle(default = Some(true),
      descrYes = "Output sequence for each k-mer in the counts table (default true)")
    val superkmers = opt[Boolean](default = Some(false),
      descr = "Instead of k-mers and counts, output human-readable superkmers and minimizers")
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

    def run(): Unit = {
      val groupedSegments = discount.kmers(inFiles() : _*).segments
      def counting = groupedSegments.counting(min.toOption, max.toOption)

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
    val output = opt[String](descr = "Location where k-mer stats are written (optional)")

    validate(inFiles) { ifs =>
      if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run(): Unit = {
      val kmers = discount.kmers(inFiles(): _*)
      kmers.showStats(min.toOption, max.toOption, output.toOption)
    }
  }
  addSubcommand(stats)
}

/**
 * Main API entry point for Discount.
 * Also see the command line examples in the documentation for more information on these options.
 *
 * @param k                 k-mer length
 * @param minimizerSet      source of minimizers. See [[Source]]
 * @param m                 minimizer width
 * @param ordering          minimizer ordering (frequency/lexicographic/given/random/signature)
 * @param sample            sample fraction for frequency orderings
 * @param maxSequenceLength max length of a single sequence (short reads)
 * @param normalize         whether to normalize k-mer orientation during counting. Causes every sequence to be scanned
 *                          in both forward and reverse, after which only forward orientation k-mers are kept.
 * @param method            counting method to use (or None for automatic selection)
 * @param spark
 */
final case class Discount(k: Int, minimizerSet: Source = minimizers.Bundled, m: Int = 10,
                          ordering: String = "frequency", sample: Double = 0.01, maxSequenceLength: Int = 1000000,
                          normalize: Boolean = false, method: Option[CountMethod] = None)(implicit spark: SparkSession) {
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
  def inputReader(files: String*) = new Inputs(files, k, maxSequenceLength)

  /** Load reads/sequences from files according to the settings in this object.
   * @param files  Input files
   * @param sample Sample fraction, if any
   * @param addRCReads Whether to add reverse complements
   */
  def getInputSequences(files: Seq[String], sample: Option[Double], addRCReads: Boolean): Dataset[NTSeq] =
    getInputFragments(files, sample, addRCReads).map(_.nucleotides)

  /** Single file version of the same method */
  def getInputSequences(file: String, sample: Option[Double] = None, addRCReads: Boolean = false): Dataset[NTSeq] =
    getInputSequences(List(file), sample, addRCReads)

  /** Load input fragments (with sequence title and location) according to the settings in this object.
   * @param files Input files
   * @param sample Sample fraction, if any
   * @param addRCReads Whether to add reverse complements
   */
  def getInputFragments(files: Seq[String], sample: Option[Double], addRCReads: Boolean): Dataset[InputFragment] =
    inputReader(files: _*).getInputFragments(addRCReads, sample)

  /** Single file version of the same method */
  def getInputFragments(file: String, sample: Option[Double] = None, addRCReads: Boolean = false): Dataset[InputFragment] =
    getInputFragments(List(file), sample, addRCReads)

  def sequenceTitles(input: String*): Dataset[SeqTitle] =
    inputReader(input :_*).getSequenceTitles

  /** Construction method that respects the ordering in templateSpace */
  private def getFrequencySpace(inFiles: List[String], validMotifs: Seq[NTSeq],
                                persistHashLocation: Option[String] = None): MotifSpace = {
    val validSetTemplate = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    val input = getInputSequences(inFiles, Some(sample), normalize)
    sampling.createSampledSpace(input, validSetTemplate, sample, persistHashLocation)
  }

  /** More efficient construction method that ignores templateSpace.
   * The ordering of validMotifs will be preserved in the case of equally frequent motifs.
   */
  private def getFrequencySpaceNoTemplate(inFiles: List[String], validMotifs: Array[NTSeq],
                                persistHashLocation: Option[String] = None): MotifSpace = {
    val validSetTemplate = MotifSpace.using(validMotifs)
    val input = getInputSequences(inFiles, Some(sample), normalize)
    sampling.createSampledSpace(input, validSetTemplate, sample, persistHashLocation)
  }


  /** Construct a read splitter for the given input files based on the settings in this object.
   *
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   */
  def getSplitter(inFiles: Option[Seq[String]], persistHash: Option[String] = None): MinSplitter = {
    val theoreticalMax = 1L << (m * 2) // 4 ^ m
    val validMotifs = minimizerSet match {
      case minimizers.Path(ml) =>
        val use = sampling.readMotifList(ml, k, m)
        println(s"${use.length}/$theoreticalMax $m-mers will become minimizers (loaded from $ml)")
        use
      case minimizers.Bundled =>
        BundledMinimizers.getMinimizers(k, m) match {
          case Some(internalMinimizers) =>
            println (s"${internalMinimizers.length}/$theoreticalMax $m-mers will become minimizers(loaded from classpath)")
            internalMinimizers
          case _ =>
            throw new Exception(s"No classpath minimizers found for k=$k, m=$m. Please specify minimizers with --minimizers\n" +
              "or --allMinimizers for all m-mers.")
        }
      case minimizers.All =>
        templateSpace.byPriority
    }

    val useSpace = ordering match {
      case "given" => MotifSpace.using(validMotifs)
      case "frequency" =>
        getFrequencySpaceNoTemplate(
          inFiles.getOrElse(throw new Exception("Frequency sampling can only be used with input data")).toList,
          validMotifs, persistHash)
      case "lexicographic" =>
        //template is lexicographically ordered by construction
        MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
      case "random" =>
        Orderings.randomOrdering(
          MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
        )
      case "signature" =>
        //Signature lexicographic
        Orderings.minimizerSignatureSpace(templateSpace)
      case "signatureFrequency" =>
        val frequencyTemplate = getFrequencySpace(
          inFiles.getOrElse(throw new Exception("Frequency sampling can only be used with input data")).toList,
          templateSpace.byPriority, persistHash)
        Orderings.minimizerSignatureSpace(frequencyTemplate)
    }
    MinSplitter(useSpace, k)
  }

  /** Load k-mers from the given files. */
  def kmers(inFiles: String*): Kmers =
    new Kmers(this, inFiles, None)

  /** Sample a fraction of k-mers from the given files. */
  def sampledKmers(fraction: Double, inFiles: String*): Kmers =
    new Kmers(this, inFiles, Some(fraction))
}

/**
 * Convenience methods for interacting with k-mers from a set of input files.
 * @param discount The Discount object
 * @param inFiles Input files
 * @param fraction Fraction of the k-mers to sample, or None for all data
 * @param spark
 */
class Kmers(val discount: Discount, val inFiles: Seq[String], fraction: Option[Double] = None)
           (implicit spark: SparkSession) {
  /** The read splitter associated with this set of inputs. */
  lazy val spl: MinSplitter = discount.getSplitter(Some(inFiles))

  /** Broadcast of the read splitter associated with this set of inputs. */
  lazy val bcSplit: Broadcast[MinSplitter] = spark.sparkContext.broadcast(spl)

  /** The overall method used for k-mer counting. If not specified, this will be guessed
   * from the input data according to a heuristic. */
  lazy val method: CountMethod = {
    discount.method match {
      case Some(m) => m
      case None =>
        //Auto-detect method
        //This is currently a very basic heuristic - to be refined over time
        val r = if (spl.space.largeBuckets.nonEmpty) Pregrouped(discount.normalize) else Simple(discount.normalize)
        println(s"Counting method: $r (use --method to override)")
        r
    }
  }

  /** Input fragments associated with these inputs. */
  def inputFragments: Dataset[InputFragment] =
    discount.getInputFragments(inFiles, fraction, discount.normalize)

  def sequenceTitles: Dataset[SeqTitle] =
    discount.sequenceTitles(inFiles: _*)

  /** Grouped segments generated from the input, which enable further processing, such as k-mer counting.
   */
  lazy val segments: GroupedSegments =
    GroupedSegments.fromReads(discount.getInputSequences(inFiles, fraction, method.addRCToMainData),
      method, bcSplit)

  /** Convenience method to obtain a counting object for these k-mers. K-mer orientations will be normalized
   * if the Discount object was configured for this.
   *
   * @param min Lower bound for counting
   * @param max Upper bound for counting
   */
  def counting(min: Option[Abundance] = None, max: Option[Abundance] = None): GroupedSegments#Counting =
    segments.counting(min, max, discount.normalize)

  /** Cache the segments. */
  def cache(): this.type = { segments.cache(); this }

  /** Unpersist the segments. */
  def unpersist(): this.type = { segments.unpersist(); this }

  /** Sample the input data, counting minimizers and writing the generated frequency ordering to HDFS.
   * @param writeLocation Location to write the frequency ordering to
   * @return A splitter object corresponding to the generated ordering
   */
  def constructSampledMinimizerOrdering(writeLocation: String): MinSplitter =
    discount.getSplitter(Some(inFiles), Some(writeLocation))

  /** Convenience method to show stats for this dataset.
   * @param min Lower bound for counting
   * @param max Upper bound for counting
   * @param outputLocation Location to optionally write the output as a file
   */
  def showStats(min: Option[Abundance] = None, max: Option[Abundance] = None,
                outputLocation: Option[String]) =
    Counting.showStats(counting(min, max).bucketStats, outputLocation)
}

object Discount extends SparkTool("Discount") {
  def main(args: Array[String]): Unit = {
    Commands.run(new DiscountConf(args)(spark))
  }
}