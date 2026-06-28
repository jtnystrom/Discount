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

package com.jnpersson.discount

import com.jnpersson.discount.hash._
import com.jnpersson.discount.spark.Configuration
import com.jnpersson.discount.util.NTBitArray

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

    conf.output.toOption match {
      case Some(o) => writeToFile(conf, o)
      case _ => prettyOutput(conf, conf.supermers(), conf.kmers())
    }

  }

  def highlighted(data: String, pattern: String): String = {
    val lidx = data.lastIndexOf(pattern)
    val preMinimizer = data.substring(0, lidx)
    val postMinimizer = data.substring(lidx + pattern.length)
    preMinimizer + Console.BLUE + pattern + Console.RESET + postMinimizer
  }

  /** Print reads, optionally super-mers, and optionally k-mers, with highlighted minimizers. */
  def prettyOutput(conf: ReadSplitConf, supermers: Boolean, kmers: Boolean): Unit = {
    val spl = conf.getSplitter()
    val k = spl.k

    for { read <- conf.getInputSequences(conf.inFile()) } {
      val split = spl.splitEncode(read).toSeq
      var writePos = 0
      //Print a single read with highlighted minimizers
      for {(pos, rank, _, _) <- split
           pattern = spl.priorities.humanReadable(rank)
           } {
        if (writePos < pos) {
          print(read.substring(writePos, pos + 1))
        }
        //handle overlapping minimizers
        val part = if (writePos > pos) { pattern.substring(writePos - pos)} else pattern
        print(Console.BLUE + part + Console.RESET)
        writePos = pos + spl.priorities.width + 1
      }
      println(read.substring(writePos))

      if (supermers) {
        var indentSize = 0
        for {
          (pos, rank, encoded, _) <- split
          supermer = encoded.toString
          pattern = spl.priorities.humanReadable(rank)
        } {
          //Print each supermer
          val indent = " " * indentSize
          print(indent)
          println(highlighted(supermer, pattern))
          println(s"$indent(pos $pos, rank $rank, len ${supermer.length - (k - 1)} k-mers) ")
          if (kmers) {
            //print individual k-mers
            for {i <- 0 until supermer.length - (k - 1)} {
              print(" " * indentSize)
              println(highlighted(supermer.substring(i, i + k), pattern))
              indentSize += 1
            }
          } else {
            indentSize += supermer.length - (k - 1)
          }
        }
      }
    }
  }

  def writeToFile(conf: ReadSplitConf, destination: String): Unit = {
    val w = new PrintWriter(destination)
    val spl = conf.getSplitter()

    try {
      for {
        read <- conf.getInputSequences(conf.inFile())
        (_, rank, supermer, _) <- spl.splitEncode(read)
      } {
        w.println(s"${spl.priorities.humanReadable(rank)}\t${supermer.toString}")
      }
    } finally {
      w.close()
    }
  }
}

//noinspection TypeAnnotation
private class ReadSplitConf(args: Array[String]) extends Configuration(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  val output = opt[String](required = false, descr = "Output file for minimizers and super-mers (bulk mode)")

  val supermers = toggle("supermers", default = Some(true), descrYes = "Pretty-print supermers")
  val kmers = toggle("kmers", default = Some(false), descrYes = "Pretty-print k-mers")

  lazy val templateSpace = MinTable.ofLength(minimizerWidth())

  def countMotifs(scanner: ShiftScanner, input: Iterator[String]): SampledFrequencies =
    SampledFrequencies.fromReads(scanner, input)

  def getFrequencyTable(validMotifs: Seq[Int]): MinTable = {
    val input = getInputSequences(inFile())
    val allMotifTable = MinTable.ofLength(minimizerWidth())
    val template = MinTable.filteredOrdering(allMotifTable, validMotifs)

    //Count all motifs in every read in the input to establish frequencies
    val scanner = ShiftScanner(template)
    val sampled = countMotifs(scanner, input)
    println("Discovered frequencies")
    sampled.print()
    sampled.toTable()
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

  def getSplitter(): MinSplitter[MinTable] = {
    val allMotifTable = MinTable.ofLength(minimizerWidth())

    lazy val validMotifs = minimizers.toOption match {
      case Some(ml) =>
        val use = scala.io.Source.fromFile(ml).getLines().map(_.split(",")(0)).toArray
        println(s"${use.length}/${allMotifTable.byPriority.length} motifs will be used (loaded from $ml)")
        use.map(x => NTBitArray.encode(x).toInt)
      case None =>
        allMotifTable.byPriority
    }

    val useTable = ordering() match {
      case Given =>
        MinTable.usingRaw(validMotifs, minimizerWidth())
      case Frequency(_) =>
        //bySequence case not supported here
        getFrequencyTable(validMotifs)
      case Lexicographic =>
        //template is lexicographically ordered by construction
        MinTable.filteredOrdering(allMotifTable, validMotifs)
      case XORMask(mask, canonical) =>
        //canonical case not supported here
        //standardize to lexicographic ordering before randomizing, for a reproducible result
        Orderings.randomOrdering(
          MinTable.filteredOrdering(allMotifTable, validMotifs),
          mask
        )
      case Signature =>
        //Signature lexicographic
        Orderings.minimizerSignatureTable(allMotifTable)
    }
    MinSplitter(useTable, k())
  }
}

