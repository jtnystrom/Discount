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

import java.io.PrintWriter

/**
 * Minimal test program that demonstrates using the Discount API
 * to split reads into super-mers without using Spark.
 * Single-threaded, only works for FASTA files with unbroken reads.
 * It is recommended to run on small input files so that the result can be inspected manually.
 * In the output, the minimizer of each super-mer will be highlighted.
 *
 * This tool makes use of the Discount configuration class CoreConf for convenience reasons.
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
    conf.output.toOption match {
      case Some(o) => writeToFile(conf, o)
      case _ => prettyOutput(conf)
    }

  }

  def prettyOutput(conf: ReadSplitConf): Unit = {
    val spl = conf.getSplitter()
    val k = spl.k

    /*
    * Print reads and super-mers
    */
    for { read <- conf.getInputSequences(conf.inFile()) } {
      println(read)
      var indentSize = 0
      for  {
        (minimizer, supermer) <- spl.split(read)
        rank = minimizer.features.rank
        pattern = minimizer.features.pattern
      } {
        /*
         * User-friendly format with colours
         */
        val indent = " " * (indentSize)
        print(indent)
        val lidx = supermer.lastIndexOf(pattern)
        val preMinimizer = supermer.substring(0, lidx)
        val postMinimizer = supermer.substring(lidx + spl.space.width)
        println(preMinimizer + Console.BLUE + pattern + Console.RESET + postMinimizer)
        println(s"$indent${pattern} (pos ${minimizer.pos}, rank ${rank}, len ${supermer.length - (k - 1)} k-mers) ")
        indentSize += supermer.length - (k - 1)

      }
    }
  }

  def writeToFile(conf: ReadSplitConf, destination: String): Unit = {
    val w = new PrintWriter(destination)
    val spl = conf.getSplitter()
    val k = spl.k
    try {
      for {
        read <- conf.getInputSequences(conf.inFile())
        (minimizer, supermer) <- spl.split(read)
      } {
        w.println(s"${minimizer.features.pattern}\t$supermer")
      }
    } finally {
      w.close()
    }
  }
}

private class ReadSplitConf(args: Array[String]) extends Configuration(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  val output = opt[String](required = false, descr = "Output file for minimizers and super-mers (bulk mode)")

  def getFrequencySpace(inFile: String, validMotifs: Seq[String]): MotifSpace = {
    val input = getInputSequences(inFile)
    val template = MotifSpace.fromTemplateWithValidSet(templateSpace, validMotifs)
    val counter = MotifCounter(template)

    //Count all motifs in every read in the input to establish frequencies
    val scanner = new ShiftScanner(template)
    scanner.countMotifs(counter, input)
    counter.print(template, "Discovered frequencies")
    counter.toSpaceByFrequency(template)
  }

  /**
   * Read FASTA files with unbroken reads (one line per read)
   */
  def getInputSequences(input: String): Iterator[String] = {
    val degenerateAndUnknown = "[^ACTGUacgtu]+"
    scala.io.Source.fromFile(input).getLines().
      filter(!_.startsWith(">")).
      flatMap(r => r.split(degenerateAndUnknown))
  }

  def getSplitter(): MinSplitter = {
    val template = templateSpace
    val validMotifs = (minimizers.toOption match {
      case Some(ml) =>
        val use = scala.io.Source.fromFile(ml).getLines().map(_.split(",")(0)).toArray
        println(s"${use.size}/${template.byPriority.size} motifs will be used (loaded from $ml)")
        use
      case None =>
        template.byPriority
    })

    val useSpace = (ordering() match {
      case "given" =>
        MotifSpace.using(validMotifs)
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
    MinSplitter(useSpace, k())
  }
}

