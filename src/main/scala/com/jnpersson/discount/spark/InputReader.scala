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
import com.jnpersson.discount.{SeqLocation, SeqTitle}
import com.jnpersson.discount.hash.InputFragment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.postfixOps

/** A buffer with raw bytes of input data, and an associated start and end location (in the buffer)
 * as well as a sequence location.
 * bufStart and bufEnd are 0-based inclusive positions. */
private final case class BufferFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                                        bufStart: Int, bufEnd: Int)

private final case class FragmentParser(k: Int, sample: Option[Double], maxSize: Int) {
  //Split very long input sequences into more manageable fragments
  //Helpful for sampling and memory usage

  val FIRST_LOCATION = 1

  def toFragments(record: Record): Iterator[InputFragment] =
    splitFragment(BufferFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getBuffer,
      record.getStartValue, record.getEndValue))

  def toFragments(record: QRecord): Iterator[InputFragment] =
    splitFragment(BufferFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getBuffer,
      record.getStartValue, record.getEndValue))

  def toFragments(partialSeq: PartialSequence): Iterator[InputFragment] = {
    val kmers = partialSeq.getBytesToProcess
    val start = partialSeq.getStartValue
    val extensionPart = new String(partialSeq.getBuffer, start + kmers, k - 1)
    val newlines = extensionPart.count(_ == '\n')

    //Newlines will be removed eventually, however we have to compensate for them here
    //to include all k-mers properly
    //Note: we assume that the extra part does not contain newlines itself

    //Although this code is general, for more than one newline in this part (as the case may be for a large k),
    //deeper changes to Fastdoop may be needed.
    val end = start + kmers + (k - 1) + newlines + 1
    val key = partialSeq.getKey.split(" ")(0)
    splitFragment(BufferFragment(key, partialSeq.getSeqPosition, partialSeq.getBuffer, start, end))
  }

  /**
   * Split a BufferFragment into manageable parts and generate valid InputFragments with (k - 1) length overlaps,
   * handling newlines properly. This avoids allocating and processing huge strings when sequences are long.
   * @param fragment
   * @return
   */
  def splitFragment(fragment: BufferFragment): Iterator[InputFragment] = {
    val all = for {
      start <- fragment.bufStart.to(fragment.bufEnd, maxSize).iterator
      end = start + maxSize - 1
      useEnd = if (end > fragment.bufEnd) { fragment.bufEnd } else { end }
      if (sample.isEmpty || Math.random() < sample.get)
      part = InputFragment(fragment.header, 0, new String(fragment.buffer, start, useEnd - start + 1))
     } yield removeNewlines(part)

    if (all.isEmpty) {
      Iterator.empty
    } else {
      val first = all.next()
      //We set fragment locations only after removing newlines, ensuring that sequence positions are correct.
      val withLocations = all.scanLeft(first.copy(location = fragment.location)) { case (acc, f) =>
        f.copy(location = acc.location + acc.nucleotides.length) }

      //Empty fragment to pad the final pair, so that sliding() works
      (withLocations ++ Iterator(InputFragment("", 0, ""))).sliding(2).
        map(x => x(0).copy(nucleotides = x(0).nucleotides + x(1).nucleotides.take(k - 1))).
        filter(x => x.nucleotides.length >= k) //the final fragment is redundant if shorter than k
    }
  }

  val nonNewline = "[^\r\n]+".r

  /**
   * Remove newlines from a fragment.
   */
  def removeNewlines(fragment: InputFragment): InputFragment = {
    if (fragment.nucleotides.indexOf('\n') != -1) {
      //The repeated regex search is too expensive for short reads, so we only do it
      //if a newline is present
      val allMatches = nonNewline.findAllMatchIn(fragment.nucleotides).mkString("")
      InputFragment(fragment.header, 0, allMatches)
    } else {
      fragment
    }
  }
}

object InputReader {
  val FRAGMENT_MAX_SIZE = 1000000

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

  private val validBases = "[ACTGUactgu]+".r

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
  private def getMultiSequences(file: String, sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE)
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf).
        flatMap(x => parser.toFragments(x._2))
    } else {
      println(s"Assuming fasta format for $file, max length $maxReadLength")
      sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf).
        flatMap(x => parser.toFragments(x._2))
    }
  }

  def longReadsWarning(): Unit = {
    println("(This input format is only for reading files containing a single long sequence. For other cases, you should not use --single)")
  }

  /**
   * Read a single long sequence in parallel splits.
   * @param file
   * @return
   */
  def getSingleSequence(file: String, sample: Option[Double]): RDD[InputFragment] = {
    longReadsWarning()
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE)
    println(s"Assuming fasta format (long sequences) for $file")
    sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      conf).
      flatMap(x => parser.toFragments(x._2))
  }

  /**
   * Get the titles of all sequences present in the input files.
   * @param fileSpec
   * @param singleSequence
   * @return
   */
  def getSequenceTitles(file: String, singleSequence: Boolean = false): Dataset[SeqTitle] = {

    //Note: this operation could be made a lot more efficient, no need to create all the fragments etc.
    val raw = if(singleSequence) {
      sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
        conf).map(_._2.getKey)
    } else {
      if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
        println(s"Assuming fastq format for $file, max length $maxReadLength")
        sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf).
          map(_._2.getKey)
      } else {
        println(s"Assuming fasta format for $file, max length $maxReadLength")
        sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf).
          map(_._2.getKey)
      }
    }
    raw.toDS.distinct
  }

  /**
   * Load sequence fragments from files, optionally adding reverse complements and/or sampling.
   */
  def getInputFragments(fileSpec: String, withRC: Boolean,
                        sample: Option[Double] = None,
                        singleSequence: Boolean = false): Dataset[InputFragment] = {
    val raw = if(singleSequence)
      getSingleSequence(fileSpec, sample)
    else
      getMultiSequences(fileSpec, sample)

    val valid = ingest(raw).toDS

    if (withRC) {
      valid.flatMap(r => {
        List(r, r.copy(nucleotides = DNAHelpers.reverseComplement(r.nucleotides)))
      })
    } else {
      valid
    }
  }
}
