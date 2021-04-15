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

  lazy val routines = new Routines(spark)
}

class DiscountSparkConf(args: Array[String], spark: SparkSession) extends CoreConf(args) {
  version(s"Discount (Distributed k-mer counting tool) v${getClass.getPackage.getImplementationVersion}")
  banner("Usage:")

  def routines = new Routines(spark)

  def getFrequencySpace(inFiles: String, validMotifs: Seq[String],
                        persistHashLocation: Option[String] = None): MotifSpace = {
    val input = getInputSequences(inFiles, long(), sample.toOption)
    val tmpl = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    sample.toOption match {
      case Some(amount) => routines.createSampledSpace(input, amount, tmpl,
        numCPUs(), persistHashLocation)
      case None => templateSpace
    }
  }

  def getInputSequences(input: String, longSequences: Boolean, sample: Option[Double] = None): Dataset[String] = {
    val addRCReads = normalize()

    routines.getReadsFromFiles(input, addRCReads, maxSequenceLength(), k(), sample, longSequences)
  }

  def restoreSplitter(location: String): ReadSplitter[_] = {
    ordering() match {
      case "frequency" =>
        val space = routines.restoreSpace(location)
        new MotifExtractor(space, k())
      case _ => ???
    }
  }

  def getSplitter(inFiles: String, persistHash: Option[String] = None): ReadSplitter[_] = {
    val template = templateSpace
    val validMotifs = (minimizers.toOption match {
      case Some(ml) =>
        val use = routines.readMotifList(ml)
        println(s"${use.size}/${template.byPriority.size} motifs will be used (loaded from $ml)")
        use
      case None =>
        template.byPriority
    })

    val useSpace = (ordering() match {
      case "frequency" =>
        getFrequencySpace(inFiles, validMotifs, persistHash)
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
        val frequencyTemplate = getFrequencySpace(inFiles, template.byPriority, persistHash)
        Orderings.minimizerSignatureSpace(frequencyTemplate)
    })
    MotifExtractor(useSpace, k())
  }

  val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files (FASTA or FASTQ format, uncompressed)")
  val min = opt[Long](descr = "Filter for minimum k-mer abundance, e.g. 2", noshort = true)
  val max = opt[Long](descr = "Filter for maximum k-mer abundance, e.g. 100", noshort = true)

  def getCounting(): Counting[_] = {
    val inData = inFiles().mkString(",")
    val spl = getSplitter(inData)
    new SimpleCounting(spark, spl, min.toOption, max.toOption, normalize())
  }

  val count = new RunnableCommand("count") {
    banner("Count k-mers in files, writing the results to a table.")
    val output = opt[String](descr = "Location (directory name prefix) where outputs are written", required = true)
    val tsv = opt[Boolean](default = Some(false), descr = "Use TSV output format instead of FASTA, which is the default")

    val sequence = toggle(default = Some(true),
      descrYes = "Output sequence for each k-mer in the counts table (default: yes)")
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
      val inData = inFiles().mkString(",")
      val input = getInputSequences(inData, long())
      val counting = getCounting()

      if (buckets()) {
        counting.writeBucketStats(input, output())
      } else {
        counting.writeCountedKmers(input, sequence(), histogram(), output(), tsv())
      }
    }
  }
  addSubcommand(count)

  val stats = new RunnableCommand("stats") {
    banner("Show summary statistics for k-mers in files. This produces no output files.")
    val rawStats = opt[Boolean](default = Some(false),
      descr = "Output raw stats without counting k-mers (for debugging)", hidden = true)
    val segmentStats = opt[Boolean](default = Some(false),
      descr = "Output segment statistics (for minimizer testing)", hidden = true)

    def run(): Unit = {
      val inData = inFiles().mkString(",")
      val input = getInputSequences(inData, long())
      val counting = getCounting()
      if (!segmentStats()) {
        counting.statisticsOnly(input, rawStats())
      } else {
        counting.segmentStatsOnly(input)
      }
    }
  }
  addSubcommand(stats)

  verify()
}

object Discount extends SparkTool("Discount") {
  def main(args: Array[String]) {
    Commands.run(new DiscountSparkConf(args, spark))
  }
}
