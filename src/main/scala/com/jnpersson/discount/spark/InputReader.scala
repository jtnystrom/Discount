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

import com.jnpersson.discount.fastdoop._
import com.jnpersson.discount.util.DNAHelpers
import com.jnpersson.discount.{NTSeq}
import com.jnpersson.discount.hash.InputFragment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.postfixOps

object InputReader {
  //Split very long input sequences into more manageable fragments
  //Helpful for sampling and memory usage
  val FRAGMENT_MAX_SIZE = 100000
  val FIRST_LOCATION = 1

  /**
   * Take nucleotides from a position while skipping any newlines (up to 1)
   * that we encounter.
   * @param data
   * @param pos Position to take from
   * @param n Amount to take
   */
  private def takeNucleotides(data: NTSeq, pos: Int, n: Int): NTSeq = {
    if (data.length < pos + n) {
      data.substring(pos)
    } else {
      val r = data.substring(pos, pos + n)
      if (r.contains('\n') && data.length >= pos + n + 1) {
        data.substring(pos, pos + n + 1)
      } else {
        r
      }
    }
  }

  private def toFragments(record: Record, k: Int): Iterator[InputFragment] =
    splitFragment(InputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getValue), k)

  private def toFragments(record: QRecord, k: Int): Iterator[InputFragment] =
    splitFragment(InputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getValue), k)

  private def toFragments(partialSeq: PartialSequence, k: Int): Iterator[InputFragment] = {
    val kmers = partialSeq.getBytesToProcess
    val start = partialSeq.getStartValue
    val extensionPart = new String(partialSeq.getBuffer, start + kmers, k - 1)
    val newlines = extensionPart.count(_ == '\n')

    //Newlines will be removed eventually, however we have to compensate for them here
    //to include all k-mers properly
    //Note: we assume that the extra part does not contain newlines itself

    //Although this code is general, for more than one newline in this part (as the case may be for a large k),
    //deeper changes to Fastdoop may be needed.
    val nts = new String(partialSeq.getBuffer, start, kmers + (k - 1) + newlines)
    val key = partialSeq.getKey.split(" ")(0)
    splitFragment(InputFragment(key, FIRST_LOCATION, nts), k)
  }

  /**
   * Split a long fragment into shorter fragments of a controlled maximum size.
   * The resulting fragments will overlap by (k-1) nucleotides
   * @param fragment fragment to split
   * @param k k
   * @return
   */
  private def splitFragment(fragment: InputFragment, k: Int): Iterator[InputFragment] = {
    //first, join any line breaks so that positions in the resulting string are true
    //sequence positions
    val raw = fragment.copy(nucleotides = fragment.nucleotides.replaceAll("\n", ""))

    if (raw.nucleotides.length <= FRAGMENT_MAX_SIZE) {
      return Iterator(raw)
    }

    val key = raw.header
    val nts = raw.nucleotides
    0.until(nts.length, FRAGMENT_MAX_SIZE).iterator.map(offset => {
      val fullEnd = offset + FRAGMENT_MAX_SIZE
      val end = if (fullEnd > nts.length) { nts.length } else { fullEnd }
      //the main part of the fragment may contain newlines, but we need to ensure a (k-1) overlap part
      //without newlines in order not to miss any k-mers when we split
      val extra = takeNucleotides(nts, end, k - 1)
      InputFragment(key, raw.location + offset, nts.substring(offset, end) + extra)
    })
  }
}

/**
 * Routines for reading input data using fastdoop.
 * @param spark
 * @param k
 */
class InputReader(maxReadLength: Int, k: Int)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import InputReader._

  private val conf = new Configuration(sc.hadoopConfiguration)

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

  //Estimate for the largest string we need to read, plus some extra space.
  private val bufsiz = maxReadLength * 2 + // sequence data and quality (fastq)
    1000 //ID string and separator characters

  //Fastdoop parameter
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  private def sampleRDD[A](data: RDD[A], fraction: Option[Double]): RDD[A] = {
    fraction match {
      case Some(s) => data.sample(false, s)
      case _ => data
    }
  }

  private val validBases = "[ACTGUactgu]+"r

  /**
   * Split the fragments around unknown or invalid characters.
   */
  private def ingest(data: RDD[InputFragment]): RDD[InputFragment] = {
    val valid = this.validBases
    data.flatMap(x => {
      valid.findAllMatchIn(x.nucleotides).map(m => {
        x.copy(nucleotides = m.matched, location = x.location + m.start)
      })
    })
  }

  /**
   * Read multiple sequences from the input file.
   * @param file
   * @return
   */
  private def getMultiSequences(file: String): RDD[InputFragment] = {
    val k = this.k
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf).
        flatMap(x => toFragments(x._2, k))
    } else {
      println(s"Assuming fasta format for $file, max length $maxReadLength")
      sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf).
        flatMap(x => toFragments(x._2, k))
    }
  }

  def longReadsWarning(): Unit = {
    println("(This input format is only for reading a single long sequence. For other cases, you should not use --long)")
  }

  /**
   * Read a single long sequence in parallel splits.
   * @param file
   * @return
   */
  def getSingleSequence(file: String): RDD[InputFragment] = {
    longReadsWarning()
    println(s"Assuming fasta format (long sequences) for $file")
    val k = this.k
    sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      conf).
      flatMap(x => toFragments(x._2, k))
  }

  /**
   * Load sequences from files, optionally adding reverse complements and/or sampling.
   * Locations are currently undefined for the single long sequence format.
   */
  def getReadsFromFiles(fileSpec: String, withRC: Boolean,
                        sample: Option[Double] = None,
                        singleSequence: Boolean = false): Dataset[InputFragment] = {
    val raw = if(singleSequence)
      getSingleSequence(fileSpec)
    else
      getMultiSequences(fileSpec)

    val valid = ingest(sampleRDD(raw, sample)).toDS

    if (withRC) {
      valid.flatMap(r => {
        List(r, r.copy(nucleotides = DNAHelpers.reverseComplement(r.nucleotides)))
      })
    } else {
      valid
    }
  }
}
