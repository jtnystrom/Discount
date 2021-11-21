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
import com.jnpersson.discount.spark.InputReader.FRAGMENT_MAX_SIZE
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

/**
 * Splits longer sequences into fragments of a controlled maximum length, optionally sampling them.
 * @param k
 * @param sample
 * @param maxSize
 */
private final case class FragmentParser(k: Int, sample: Option[Double], maxSize: Int,
                                        multiline: Boolean) {

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
    if (kmers == 0) {
      return Iterator.empty
    }

    val extensionPart = new String(partialSeq.getBuffer, start + kmers, k - 1)
    val newlines = extensionPart.count(_ == '\n')

    //Newlines will be removed eventually, however we have to compensate for them here
    //to include all k-mers properly
    //Note: we assume that the extra part does not contain newlines itself

    //Although this code is general, for more than one newline in this part (as the case may be for a large k),
    //deeper changes to Fastdoop may be needed.
    //This value is 0-based inclusive of end
    val end = start + kmers - 1 + (k - 1) + newlines
    val useEnd = if (end > partialSeq.getEndValue) partialSeq.getEndValue else end

    val key = partialSeq.getKey.split(" ")(0)
    splitFragment(BufferFragment(key, partialSeq.getSeqPosition, partialSeq.getBuffer, start, useEnd))
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

      //Adjust fragments so that they have (k - 1) overlap, so that we can see all k-mers in the result.
      //But first, add an empty fragment to pad the final pair, so that sliding() works
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
    if (multiline) {
      val allMatches = nonNewline.findAllMatchIn(fragment.nucleotides).mkString("")
      InputFragment(fragment.header, 0, allMatches)
    } else {
      fragment
    }
  }
}

object InputReader {
  val FRAGMENT_MAX_SIZE = 1000000

  /**
   * Obtain an InputReader for the given file/files. If multiple files are specified, they must have
   * the same format.
   * @param file A path, or a list of paths.
   * @param k
   * @param maxReadLength Max length for short reads
   * @param singleSequence (Fasta case) whether the input is a single long sequence.
   * @param spark
   * @return
   */
  def forFile(file: String, k: Int, maxReadLength: Int, singleSequence: Boolean)
             (implicit spark: SparkSession): InputReader = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      new FastqShortReader(file, k, maxReadLength)
    } else {
      if (singleSequence) {
        //TODO fai check
        new IndexedFastaInputReader(file, k)
//        new SingleSequenceReader(file, k)
      } else {
        new FastaShortReader(file, k, maxReadLength)
      }
    }
  }
}

/**
 * Routines for reading input data using fastdoop.
 * @param spark
 * @param file
 * @param k
 */
abstract class InputReader(file: String, k: Int)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  protected val conf = new Configuration(sc.hadoopConfiguration)

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

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

  def getSequenceTitles: Dataset[SeqTitle]

  /**
   * Read sequence data as fragments from the input files, removing any newlines.
   * @param sample Sample fraction, if any
   * @return
   */
  protected def getFragments(sample: Option[Double]): RDD[InputFragment]


  /**
   * Load sequence fragments from files, optionally adding reverse complements and/or sampling.
   */
  def getInputFragments(withRC: Boolean, sample: Option[Double] = None): Dataset[InputFragment] = {
    val raw = getFragments(sample)
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

/**
 * InputReader for a single long FASTA sequence
 * @param file
 * @param k
 * @param spark
 */
class SingleSequenceReader(file: String, k: Int)(implicit spark: SparkSession) extends InputReader(file, k) {
  import spark.sqlContext.implicits._

  println(s"Assuming fasta format (long sequences) for $file")
  println("(This input format is only for reading files containing a single long sequence. For other cases, you should not use --single)")

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence], conf)

  /**
   * Read a single long sequence in parallel splits.
   * @param file
   * @return
   */
  def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE, true)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS.distinct
}

/**
 * Input reader for FASTA short reads
 * @param file
 * @param k
 * @param maxReadLength
 * @param spark
 */
class FastaShortReader(file: String, k: Int, maxReadLength: Int)(implicit spark: SparkSession)
  extends InputReader(file, k) {
  import spark.sqlContext.implicits._
  private val bufsiz = maxReadLength + // sequence data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  println(s"Assuming fasta format for $file, max length $maxReadLength")

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf)

  protected def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE, false)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS().distinct
}

/**
 * Input reader for FASTQ short reads
 * @param file
 * @param k
 * @param maxReadLength
 * @param spark
 */
class FastqShortReader(file: String, k: Int, maxReadLength: Int)(implicit spark: SparkSession) extends InputReader(file, k) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength * 2 + // sequence and quality data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  println(s"Assuming fastq format for $file, max length $maxReadLength")

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf)

  protected def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE, false)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS.distinct
}

//TODO rename this class (and possibly its siblings)
class IndexedFastaInputReader(file: String, k: Int)(implicit spark: SparkSession) extends InputReader(file, k) {
  import spark.sqlContext.implicits._
  println(s"Assuming fasta format (faidx) for $file")

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[IndexedFastaFormat], classOf[Text], classOf[PartialSequence], conf)

  /**
   * Read a single long sequence in parallel splits.
   * @param file
   * @return
   */
  def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE, true)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS.distinct
}
