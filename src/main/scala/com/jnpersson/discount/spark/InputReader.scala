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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.postfixOps

/**
 * Splits longer sequences into fragments of a controlled maximum length, optionally sampling them.
 * @param k
 * @param sample
 */

private final case class FragmentParser(k: Int) {
  def makeInputFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                        start: Int, end: Int): InputFragment = {
    val nucleotides = new String(buffer, start, end - start + 1)
    InputFragment(header, location, nucleotides)
  }

  val FIRST_LOCATION = 1

  def toFragment(record: Record): InputFragment =
    makeInputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getBuffer,
      record.getStartValue, record.getEndValue)


  def toFragment(record: QRecord): InputFragment =
    makeInputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getBuffer,
      record.getStartValue, record.getEndValue)


  def toFragment(partialSeq: PartialSequence): InputFragment = {
    val kmers = partialSeq.getBytesToProcess
    val start = partialSeq.getStartValue
    if (kmers == 0) {
      return InputFragment("", 0, "")
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
    makeInputFragment(key, partialSeq.getSeqPosition, partialSeq.getBuffer, start, useEnd)
  }
}

/**
 * A set of input files that can be parsed into [[InputFragment]]
 * @param files files to read. A name of the format @list.txt will be parsed as a list of files.
 * @param k length of k-mers
 * @param maxReadLength max length of short sequences
 * @param spark the SparkSession
 */
class Inputs(files: Seq[String], k: Int, maxReadLength: Int)(implicit spark: SparkSession) {
  protected val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
  import spark.sqlContext.implicits._

  private val expandedFiles = files.toList.flatMap(f => {
    if (f.startsWith("@")) {
      println(s"Assuming that $f is a list of input files (using @ syntax)")
      val realName = f.drop(1)
      spark.read.textFile(realName).collect()
    } else {
      List(f)
    }
  })

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
      val faiPath = file + ".fai"
      if (Util.fileExists(faiPath)) {
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
   * @param withRC whether to add reverse complement sequences
   * @param sample sample fraction, if any (None to read all data)
   * @param withAmbiguous whether to include ambiguous nucleotides. If not, the inputs will be split and only valid
   *                      nucleotides retained.
   * @return
   */
  def getInputFragments(withRC: Boolean, withAmbiguous: Boolean = false): Dataset[InputFragment] = {
    expandedFiles.map(forFile).map(_.getInputFragments(withRC, withAmbiguous)).
      reduceOption(_ union _).
      getOrElse(spark.emptyDataset[InputFragment])
  }

  /**
   * All sequence titles contained in this set of input files
   */
  def getSequenceTitles: Dataset[SeqTitle] =
    expandedFiles.map(forFile).map(_.getSequenceTitles).reduceOption(_ union _).
      getOrElse(spark.emptyDataset[SeqTitle])
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

  private val validBases = "[ACTGUactgu\n\r]+".r

  /**
   * Split the fragments around unknown or invalid characters.
   */
  private def removeInvalid(data: RDD[InputFragment]): RDD[InputFragment] = {
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
  protected def getFragments(): RDD[InputFragment]

  /**
   * Load sequence fragments from files, optionally adding reverse complements and/or sampling.
   */
  def getInputFragments(withRC: Boolean, withAmbiguous: Boolean): Dataset[InputFragment] = {
    val raw = getFragments()
    val valid = if (withAmbiguous) raw.toDS() else removeInvalid(raw).toDS()

    if (withRC && ! withAmbiguous) {
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

  protected def getFragments(): RDD[InputFragment] = {
    val parser = FragmentParser(k)
    hadoopFile.map(x => parser.toFragment(x._2))
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

  protected def getFragments(): RDD[InputFragment] = {
    val parser = FragmentParser(k)
    hadoopFile.map(x => parser.toFragment(x._2))
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
   * @param sample
   * @return
   */
  def getFragments(): RDD[InputFragment] = {
    val parser = FragmentParser(k)
    hadoopFile.map(x => parser.toFragment(x._2))
  }

  def getSequenceTitles: Dataset[SeqTitle] =
    hadoopFile.map(_._2.getKey).toDS().distinct()
}
