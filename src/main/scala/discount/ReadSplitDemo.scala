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
 * Minimal test program that demonstrates using the Hypercut API
 * to split reads into super-mers without using Spark.
 * Single-threaded, only works for FASTA files with unbroken reads.
 * It is recommended to run on small input files so that the result can be inspected manually.
 *
 * Note that this will ignore many configuration flags, for example the sample fraction
 * (will always equal 1.0 as sampling is not supported). However, in principle,
 * all the minimizer orderings supported by Discount are supported.
 *
 * Run with e.g. the following command:
 * sbt "runMain discount.ReadSplitDemo -m 10 -k 28 small.fasta"
 */
object ReadSplitDemo {


  def main(args: Array[String]): Unit = {
    val conf = new ReadSplitConf(args)
    conf.verify()
    val spl = conf.getSplitter()

    /**
     * Print reads and super-mers, highlighting locations of minimizers
     */
    for (r <- conf.getInputSequences(conf.inFile())) {
      println(s"Read: $r")
      for (s <- spl.split(r)) {
        val compact = spl.compact(s._1)
        val supermer = s._2
        val minimizer = s._1.features.pattern
        print(s"  ${minimizer} (pos ${s._1.pos}, ID ${compact}, len ${supermer.length - (spl.k - 1)} km) ")

        val idx = supermer.indexOf(minimizer)
        val preSupermer = supermer.take(idx)
        val postSupermer = supermer.drop(idx + spl.space.width)
        println(preSupermer + Console.BLUE + minimizer + Console.RESET + postSupermer)
      }
    }
  }
}

class ReadSplitConf(args: Array[String]) extends CoreConf(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  def createSampledSpace(input: Iterator[String], template: MotifSpace): MotifSpace = {
    val counter = MotifCounter(template)
    val scanner = new MotifCountingScanner(template)
    scanner.scanGroup(counter, input)
    counter.print(template, s"Discovered frequencies")
    counter.toSpaceByFrequency(template)
  }

  def getFrequencySpace(inFile: String, validMotifs: Seq[String]): MotifSpace = {
    val input = getInputSequences(inFile)
    val tmpl = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    sample.toOption match {
      case Some(amount) => createSampledSpace(input, tmpl)
      case None => templateSpace
    }
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

