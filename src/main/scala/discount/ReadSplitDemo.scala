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

package discount

import discount.hash._

/**
 * Minimal test program that demonstrates using the Discount API
 * to split reads into super-mers without using Spark.
 * Single-threaded, only works for FASTA files with unbroken reads.
 * It is recommended to run on small input files so that the result can be inspected manually.
 * In the output, the minimizer of each super-mer will be highlighted.
 *
 * This tool makes use of the Discount configuration class CoreConf for simplicity.
 * Note that this will ignore many arguments, for example the sample fraction
 * (will always equal 1.0 as true sampling is not supported). However, in principle,
 * all the minimizer orderings supported by Discount are supported.
 * This tool ignores the following arguments: --long, --maxlen, --normalize,
 * --numCPUs, --sample.
 * Unlike the full Discount, only one file can be processed.
 *
 * Run with e.g. the following command:
 * sbt "runMain discount.ReadSplitDemo -m 10 -k 28 small.fasta"
 *
 * To get help:
 * sbt "runMain discount.ReadSplitDemo --help"
 *
 */
object ReadSplitDemo {
  def main(args: Array[String]): Unit = {
    val conf = new ReadSplitConf(args)
    conf.verify()
    val spl = conf.getSplitter()

    val k = spl.k
    /**
     * Print reads and super-mers, highlighting locations of minimizers
     */
    for (r <- conf.getInputSequences(conf.inFile())) {
      println(r)
      var runLen = 0
      for (s <- spl.split(r)) {
        val rank = s._1.features.rank
        val supermer = s._2
        val minimizer = s._1.features.pattern

        val indent = " " * (runLen)
        print(indent)
        val lidx = supermer.lastIndexOf(minimizer)
        val preMinimizer = supermer.substring(0, lidx)
        val postMinimizer = supermer.substring(lidx + spl.space.width)
        println(preMinimizer + Console.BLUE + minimizer + Console.RESET + postMinimizer)
        println(s"$indent${minimizer} (pos ${s._1.pos}, rank ${rank}, len ${supermer.length - (k - 1)} k-mers) ")
        runLen += supermer.length - (k - 1)
      }
    }
  }
}

class ReadSplitConf(args: Array[String]) extends CoreConf(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  def getFrequencySpace(inFile: String, validMotifs: Seq[String]): MotifSpace = {
    val input = getInputSequences(inFile)
    val template = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    val counter = MotifCounter(template)
    val scanner = new MotifCountingScanner(template)

    //Count all motifs in every read in the input to establish frequencies
    scanner.scanGroup(counter, input)
    counter.print(template, s"Discovered frequencies")
    counter.toSpaceByFrequency(template)
  }

  /**
   * Read FASTA files with unbroken reads (one line per read)
   */
  def getInputSequences(input: String): Iterator[String] = {
    val degenerateAndUnknown = "[^ACTGU]+"
    scala.io.Source.fromFile(input).getLines().
      filter(!_.startsWith(">")).
      flatMap(r => r.split(degenerateAndUnknown))
  }

  def getSplitter(): MotifExtractor = {
    val template = templateSpace
    val validMotifs = (minimizers.toOption match {
      case Some(ml) =>
        val use = scala.io.Source.fromFile(ml).getLines().toArray
        println(s"${use.size}/${template.byPriority.size} motifs will be used (loaded from $ml)")
        use
      case None =>
        template.byPriority
    })

    val useSpace = (ordering() match {
      case "frequency" =>
        getFrequencySpace(inFile(), validMotifs)
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
        val frequencyTemplate = getFrequencySpace(inFile(), template.byPriority)
        Orderings.minimizerSignatureSpace(frequencyTemplate)
    })
    MotifExtractor(useSpace, k())
  }
}

