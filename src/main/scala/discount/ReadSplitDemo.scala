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
import discount.spark.Counting
import discount.util.{NTBitArray, ZeroNTBitArray}

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, FileInputStream, FileOutputStream, FileWriter, ObjectOutputStream, PrintWriter}
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

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
      case Some(o) => writeToFile(conf, o, conf.binary())
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

  def writeToFile(conf: ReadSplitConf, destination: String, binary: Boolean): Unit = {
    if (binary) {
      writeToFileBinary(conf, destination)
    } else {
      writeToFileText(conf, destination)
    }
  }

  def writeToFileText(conf: ReadSplitConf, destination: String): Unit = {
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

  def writeToFileBinary(conf: ReadSplitConf, destination: String): Unit = {
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(destination)))
    val spl = conf.getSplitter()
    val k = spl.k
    try {
      for {
        read <- conf.getInputSequences(conf.inFile())
        (minimizer, supermer) <- spl.split(read)
        enc = NTBitArray.encode(supermer)
      } {
        out.writeInt(minimizer.rank)
        val longs = enc.data.length
        val supermerLength = enc.size.toShort
        out.writeShort(supermerLength)
        for { i <- 0 until longs } out.writeLong(enc.data(i))
      }
    } finally {
      out.close()
    }
  }

  def readBinaryFile(location: String): Array[(Int, ZeroNTBitArray)] = {
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(location)))
    val initSize = 1000000
    val resultBuffer = new ArrayBuffer[(Int, ZeroNTBitArray)](initSize)

    try {
        while(true) {
          val minimizer = in.readInt()
          val supermerLength = in.readShort()
          val longs = if (supermerLength % 32 == 0) { supermerLength / 32 } else { supermerLength / 32 + 1}
          val longBuffer = new Array[Long](longs)
          for {i <- 0 until longs} {
            longBuffer(i) = in.readLong()
          }
          resultBuffer += ((minimizer, ZeroNTBitArray(longBuffer, supermerLength)))
        }
    } catch {
      case eof: EOFException => //expected
      case e: Exception => e.printStackTrace()
    } finally {
      in.close()
    }
    resultBuffer.toArray
  }
}

class ReadSplitConf(args: Array[String]) extends CoreConf(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (FASTA)")

  val output = opt[String](descr = "Output file for minimizers and super-mers (bulk mode)")

  val binary = opt[Boolean](descr = "Binary output", default = Some(false))

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

class CountKmersConf(args: Array[String]) extends CoreConf(args) {
  val inFile = trailArg[String](required = true, descr = "Input file (binary)")

  val output = opt[String](descr = "Output location")
}

/**
 * Tool to read binary encoded super-mers and count the k-mers, generating the full count table.
 * Run example:
 *  sbt -J-Xmx2g  "runMain discount.CountKmers -k 28 1M_supermers_bin.txt  --output 1M_scala_bin_out.txt"
 *  (-J-Xmx2g sets the maximum heap size of the process)
 */
object CountKmers {

  def main(args: Array[String]): Unit = {
    val conf = new ReadSplitConf(args)
    conf.verify()
    val k = conf.k()
    val writer = new PrintWriter(conf.output())

    val data = ReadSplitDemo.readBinaryFile(conf.inFile())
    try {
      //Group by in memory - doesn't scale to big data, so we will eventually compensate by controlling file sizes
      val byMinimizer = data.groupBy(_._1)
      for {(minimizer, supermers) <- byMinimizer} {
        val forwardOnly = false
        val counts = Counting.countsFromSequences(supermers.map(_._2), k, forwardOnly)

        //Reuse the byte buffer and string builder as much as possible
        //The strings generated here are a big source of memory pressure.
        val buffer = ByteBuffer.allocate(k / 4 + 8) //space for up to 1 extra long
        val builder = new StringBuilder(k)
        for {(kmer, count) <- counts} {
          val kmerSequence = NTBitArray.longsToString(buffer, builder, kmer, 0, k)
          writer.println(s"$kmerSequence\t$count")
        }
      }
    } finally {
      writer.close()
    }
  }
}
