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

package com.jnpersson.discount.spark

import com.jnpersson.discount.fastdoop._
import com.jnpersson.discount.util.{DNAHelpers, InvalidNucleotideException}
import com.jnpersson.discount.{SeqLocation, SeqTitle}
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.InputReader.FRAGMENT_MAX_SIZE
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.postfixOps

/** A buffer with raw bytes of input data, and an associated start and end location (in the buffer)
 * as well as a sequence location. May contain whitespace such as newlines.
 * bufStart and bufEnd are 0-based inclusive positions. */
private final case class BufferFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                                        bufStart: Int, bufEnd: Int)

/**
 * Splits longer sequences into fragments of a controlled maximum length, optionally sampling them.
 * @param k
 * @param sample
 * @param maxSize
 */
private final case class FragmentParser(k: Int, sample: Option[Double], maxSize: Int) {

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
   * Split a BufferFragment into subfragments of controlled size and generate valid
   * InputFragments with (k - 1) length overlaps, handling newlines properly.
   * This avoids allocating and processing huge strings when sequences are long.
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
      //After removing newlines, we can easily set the location of each fragment.
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
    if (fragment.nucleotides.indexOf('\n') != -1) {
      //The repeated regex search is too expensive for short reads, so we only do it
      //if at least one newline is present
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
 * A set of input files that can be parsed into [[InputFragment]]
 * @param files
 * @param k
 * @param maxReadLength
 * @param spark
 */
class Inputs(files: Seq[String], k: Int, maxReadLength: Int)(implicit spark: SparkSession) {
  protected val conf = new Configuration(spark.sparkContext.hadoopConfiguration)

  /**
   * By looking at the file name and checking for the presence of a .fai file in the case of fasta,
   * obtaine an appropriate InputReader for a single file.
   * @param file
   * @return
   */
  def forFile(file: String): InputReader = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      new FastqShortInput(file, k, maxReadLength)
    } else {
      //Assume fasta format
      val faiPath = new Path(file + ".fai")
      val fs = faiPath.getFileSystem(conf)
      if (fs.exists(faiPath)) {
        println(s"$faiPath found. Using indexed fasta format for $file")
        new IndexedFastaInput(file, k)
      } else {
        println(s"$faiPath not found. Assuming simple fasta format for $file, max length $maxReadLength")
        new FastaShortInput(file, k, maxReadLength)
      }
    }
  }

  /**
   * Parse all files in this set as InputFragments
   * @param withRC Whether to add reverse complement sequences
   * @param sample Sample fraction, if any (None to read all data)
   * @return
   */
  def getInputFragments(withRC: Boolean, sample: Option[Double] = None): Dataset[InputFragment] =
    files.map(forFile).map(_.getInputFragments(withRC, sample)).reduce(_ union _)

  /**
   * All sequence titles contained in this set of input files
   */
  def getSequenceTitles: Dataset[SeqTitle] =
    files.map(forFile).map(_.getSequenceTitles).reduce(_ union _)
}

/**
 * A reader that reads input data from one file using a specific Hadoop format
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

  /**
   * Sequence titles in this file
   * @return
   */
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
    val valid = ingest(raw).toDS()

    if (withRC) {
        valid.flatMap(r => {
          try {
            List(r, r.copy(nucleotides = DNAHelpers.reverseComplement(r.nucleotides)))
          } catch {
            case ine: InvalidNucleotideException =>
              Console.err.println(s"Invalid nucleotides in sequence with header: ${r.header}")
              Console.err.println(s"Offending character: ${ine.invalidChar}")
              Console.err.println("Sequence: " + r.nucleotides)
              throw ine
          }
        })
    } else {
      valid
    }
  }
}

/**
 * Input reader for FASTA sequences of a fixed maximum length.
 * Uses [[FASTAshortInputFileFormat]]
 * @param file
 * @param k
 * @param maxReadLength
 * @param spark
 */
class FastaShortInput(file: String, k: Int, maxReadLength: Int)(implicit spark: SparkSession)
  extends InputReader(file, k) {
  import spark.sqlContext.implicits._
  private val bufsiz = maxReadLength + // sequence data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf)

  protected def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS().distinct()
}

/**
 * Input reader for FASTQ short reads. Uses [[FASTQInputFileFormat]]
 * @param file
 * @param k
 * @param maxReadLength
 * @param spark
 */
class FastqShortInput(file: String, k: Int, maxReadLength: Int)(implicit spark: SparkSession) extends InputReader(file, k) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength * 2 + // sequence and quality data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf)

  protected def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS().distinct()
}

/**
 * Input reader for FASTA files containing potentially long sequences, with a .fai index
 * FAI indexes can be created with tools such as seqkit.
 * Uses [[IndexedFastaFormat]]
 *
 * @param file
 * @param k
 * @param spark
 */
class IndexedFastaInput(file: String, k: Int)(implicit spark: SparkSession) extends InputReader(file, k) {
  import spark.sqlContext.implicits._

  private def hadoopFile =
    sc.newAPIHadoopFile(file, classOf[IndexedFastaFormat], classOf[Text], classOf[PartialSequence], conf)

  /**
   * Read a single long sequence in parallel splits.
   * @param file
   * @return
   */
  def getFragments(sample: Option[Double]): RDD[InputFragment] = {
    val parser = FragmentParser(k, sample, FRAGMENT_MAX_SIZE)
    hadoopFile.flatMap(x => parser.toFragments(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS().distinct()
}
