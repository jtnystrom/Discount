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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import discount._
import discount.hash._

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    //SparkConf can be customized here if needed
    new SparkConf
  }

  lazy val spark =
    SparkSession.builder().appName(appName).
      enableHiveSupport().
      master("spark://localhost:7077").config(conf).getOrCreate()
}


abstract class SparkToolConf(args: Array[String])(implicit spark: SparkSession) extends CoreConf(args) {
  def sampling = new Sampling

  lazy val discount =
    new Discount(k(), minimizers.toOption, minimizerWidth(), ordering(), sample(), samplePartitions(),
      maxSequenceLength(), multiline(), long(), rna(),
      normalize())

  def getIndexSplitter(location: String): MotifExtractor = {
    val minLoc = s"${location}_minimizers"
    val use = sampling.readMotifList(s"${location}_minimizers")
    println(s"${use.size} motifs will be used (loaded from $minLoc)")
    MotifExtractor(MotifSpace.using(use), k())
  }
}

class DiscountSparkConf(args: Array[String])(implicit spark: SparkSession) extends SparkToolConf(args) {
  version(s"Discount (Distributed k-mer counting tool) v${getClass.getPackage.getImplementationVersion}")
  banner("Usage:")

  val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
  val min = opt[Long](descr = "Filter for minimum k-mer abundance", noshort = true)
  val max = opt[Long](descr = "Filter for maximum k-mer abundance", noshort = true)

  val count = new RunnableCommand("count") {
    val output = opt[String](descr = "Location where outputs are written", required = true)

    val tsv = opt[Boolean](default = Some(false), descr = "Use TSV output format instead of FASTA, which is the default")

    val sequence = toggle(default = Some(true),
      descrYes = "Output sequence for each k-mer in the counts table (default true)")
    val superkmers = opt[Boolean](default = Some(false),
      descr = "Instead of k-mers, output human-readable superkmers in the counts table")
    val histogram = opt[Boolean](default = Some(false),
      descr = "Output a histogram instead of a counts table")
    val buckets = opt[Boolean](default = Some(false),
      descr = "Instead of k-mer counts, output per-bucket summaries (for minimizer testing)")

    validate(tsv, histogram, sequence) { (t, h, s) =>
      if (h && !t) Left("Histogram output requires TSV format (--tsv)")
      else if (!s && !t) Left("FASTA output requires --sequence")
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
      } else {
        counting.counts.write(sequence(), output(), tsv())
      }
    }
  }
  addSubcommand(count)

  val stats = new RunnableCommand("stats") {
    def run(): Unit = {
      val kmers = discount.kmers(inFiles())
      kmers.showStats(min.toOption, max.toOption)
    }
  }
  addSubcommand(stats)

  verify()
}


/**
 * Main API entry point for Discount.
 * Also see CoreConf and the command line examples for more information on these options.
 *
 * @param k                 k-mer length
 * @param minimizersFile    location of universal k-mer hitting set (or a directory with multiple sets)
 * @param m                 minimizer width
 * @param ordering          minimizer ordering
 * @param sample            sample fraction for frequency orderings
 * @param rna               RNA mode instead of DNA
 * @param normalize         whether to normalize k-mer orientation during counting
 * @param longSequences     long sequences instead of short reads
 * @param maxSequenceLength max length of a single sequence (short reads)
 * @param multiline         multiline FASTA mode
 * @param samplePartitions  number of partitions to use for frequency sampling
 *                          (suggested value: number of CPUs on workers)
 * @param spark
 */
case class Discount(val k: Int, val minimizersFile: Option[String], val m: Int = 10, val ordering: String = "frequency",
                    val sample: Double = 0.01, val samplePartitions: Int = 4,
                    val maxSequenceLength: Int = 1000, val multiline: Boolean = false,
                    val longSequences: Boolean = false,
                    val rna: Boolean = false,  val normalize: Boolean = false
                   )(implicit spark: SparkSession) {
  def sampling = new Sampling

  lazy val templateSpace = MotifSpace.ofLength(m, rna)

  def inputReader = new InputReader(maxSequenceLength, k, multiline)

  def getInputSequences(input: String, sample: Option[Double] = None): Dataset[NTSeq] = {
    val addRCReads = normalize
    inputReader.getReadsFromFiles(input, addRCReads, sample, longSequences)
  }

  def getFrequencySpace(inFiles: String, validMotifs: Seq[String],
                        persistHashLocation: Option[String] = None): MotifSpace = {
    val validSetTemplate = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    val input = getInputSequences(inFiles, Some(sample))
    sampling.createSampledSpace(input, validSetTemplate, samplePartitions, persistHashLocation)
  }

  def getSplitter(inFiles: Option[String], persistHash: Option[String] = None): MotifExtractor = {
    val template = templateSpace
    val validMotifs = (minimizersFile match {
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
    MotifExtractor(useSpace, k)
  }

  /**
   * Load k-mers from the given files.
   * @param inFiles
   * @return
   */
  def kmers(inFiles: List[String]): Kmers =
    new Kmers(this, inFiles)

  /**
   * Load k-mers from the given file.
   * @param inFile
   * @return
   */
  def kmers(inFile: String): Kmers =
    kmers(List(inFile))

}

/**
 * Convenience methods for interacting with k-mers from a set of input files.
 * @param discount
 * @param inFiles
 * @param spark
 */
class Kmers(discount: Discount, inFiles: List[String])(implicit spark: SparkSession) {
  private def inData = inFiles.mkString(",")

  lazy val spl = discount.getSplitter(Some(inData))
  lazy val bcSplit = spark.sparkContext.broadcast(spl)

  val segments: GroupedSegments =
    GroupedSegments.fromReads(discount.getInputSequences(inData), bcSplit)

  /**
   * Cache the segments.
   * @return
   */
  def cache(): this.type = { segments.cache(); this }

  /**
   * Unpersist the segments.
   * @return
   */
  def unpersist(): this.type = { segments.unpersist(); this }

  /**
   * Sample the input data, writing the generated frequency ordering to HDFS.
   * @param writeLocation Location to write the frequency ordering to
   * @return
   */
  def sample(writeLocation: String): MotifExtractor =
    discount.getSplitter(Some(inData), Some(writeLocation))

  /**
   * Convenience method to show stats for this dataset.
   * @param min
   * @param max
   */
  def showStats(min: Option[Abundance] = None, max: Option[Abundance] = None) =
    Counting.showStats(segments.counting(min, max).bucketStats)
}

object Discount extends SparkTool("Discount") {
  def main(args: Array[String]) {
    Commands.run(new DiscountSparkConf(args)(spark))
  }
}
