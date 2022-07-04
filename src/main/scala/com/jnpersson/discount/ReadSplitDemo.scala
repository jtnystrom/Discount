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

package com.jnpersson.discount

import com.jnpersson.discount.hash._

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
 * --numCPUs, --sample. Support for other arguments may be partial.
 * Unlike the full Discount, only one file can be processed.
 *
 * Run with e.g. the following command:
 * sbt "runMain com.jnpersson.discount.ReadSplitDemo -m 10 -k 28 small.fasta"
 *
 * To get help:
 * sbt "runMain com.jnpersson.discount.ReadSplitDemo --help"
 *
 * This tool is only a demo and currently ignores the following parameters: --maxlen, --normalize,
 * --sample.
 */
object ReadSplitDemo {
  def main(args: Array[String]): Unit = {
    val conf = new ReadSplitConf(args)
    conf.verify()
    val spl = conf.getSplitter()

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
        (pos, rank, encoded, _) <- spl.splitEncode(read)
        supermer = encoded.toString
        pattern = spl.priorities.humanReadable(rank)
      } {
        /*
         * User-friendly format with colours
         */
        val indent = " " * indentSize
        print(indent)
        val lidx = supermer.lastIndexOf(pattern)
        val preMinimizer = supermer.substring(0, lidx)
        val postMinimizer = supermer.substring(lidx + spl.priorities.width)
        println(preMinimizer + Console.BLUE + pattern + Console.RESET + postMinimizer)
        println(s"$indent$pattern (pos $pos, rank $rank, len ${supermer.length - (k - 1)} k-mers) ")
        indentSize += supermer.length - (k - 1)

      }
    }
  }

  def writeToFile(conf: ReadSplitConf, destination: String): Unit = {
    val w = new PrintWriter(destination)
    val spl = conf.getSplitter()

    try {
      for {
        read <- conf.getInputSequences(conf.inFile())
        (pos, rank, supermer, _) <- spl.splitEncode(read)
        m = rank.toInt
      } {
        w.println(s"${spl.priorities.humanReadable(m)}\t${supermer.toString}")
      }
    } finally {
      w.close()
    }
  }
}

private class ReadSplitConf(args: Array[String]) extends Configuration(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  val output = opt[String](required = false, descr = "Output file for minimizers and super-mers (bulk mode)")
  lazy val templateSpace = MotifSpace.ofLength(minimizerWidth())

  def countMotifs(scanner: ShiftScanner, input: Iterator[String]): SampledFrequencies =
    SampledFrequencies.fromReads(scanner, input)

  def getFrequencySpace(inFile: String, validMotifs: Seq[String]): MotifSpace = {
    val input = getInputSequences(inFile)
    val allMotifSpace = MotifSpace.ofLength(minimizerWidth())
    val template = MotifSpace.fromTemplateWithValidSet(allMotifSpace, validMotifs)

    //Count all motifs in every read in the input to establish frequencies
    val scanner = ShiftScanner(template)
    val sampled = countMotifs(scanner, input)
    println("Discovered frequencies")
    sampled.print()
    sampled.toSpace(1)
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

  def getSplitter(): MinSplitter[MotifSpace] = {
    val allMotifSpace = MotifSpace.ofLength(minimizerWidth())
    val validMotifs = minimizers.toOption match {
      case Some(ml) =>
        val use = scala.io.Source.fromFile(ml).getLines().map(_.split(",")(0)).toArray
        println(s"${use.length}/${allMotifSpace.byPriority.length} motifs will be used (loaded from $ml)")
        use
      case None =>
        allMotifSpace.byPriority
    }

    val useSpace = ordering() match {
      case "given" =>
        MotifSpace.using(validMotifs)
      case "frequency" =>
        getFrequencySpace(inFile(), validMotifs)
      case "lexicographic" =>
        //template is lexicographically ordered by construction
        MotifSpace.fromTemplateWithValidSet(allMotifSpace, validMotifs)
      case "random" =>
        Orderings.randomOrdering(
          MotifSpace.fromTemplateWithValidSet(allMotifSpace, validMotifs)
        )
      case "signature" =>
        //Signature lexicographic
        Orderings.minimizerSignatureSpace(allMotifSpace)
      case "signatureFrequency" =>
        val frequencyTemplate = getFrequencySpace(inFile(), allMotifSpace.byPriority)
        Orderings.minimizerSignatureSpace(frequencyTemplate)
    }
    MinSplitter(useSpace, k())
  }
}

