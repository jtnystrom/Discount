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

package com.jnpersson.discount.spark

import com.jnpersson.discount.fastdoop._
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.util.DNAHelpers
import com.jnpersson.discount.{SeqLocation, SeqTitle}
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.postfixOps


/**
 * Splits longer sequences into fragments of a controlled maximum length, optionally sampling them.
 * @param k length of k-mers
 */

private final case class FragmentParser(k: Int) {
  def makeInputFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                        start: Int, end: Int): InputFragment = {
    val nucleotides = new String(buffer, start, end - start + 1)
    InputFragment(header, location, nucleotides, None)
  }

  val FIRST_LOCATION = 1

  def toFragment(record: AnyRef): InputFragment = record match {
      case rec: Record =>
        makeInputFragment(rec.getKey.split(" ")(0), FIRST_LOCATION, rec.getBuffer,
          rec.getStartValue, rec.getEndValue)
      case qrec: QRecord =>
        makeInputFragment(qrec.getKey.split(" ")(0), FIRST_LOCATION, qrec.getBuffer,
          qrec.getStartValue, qrec.getEndValue)
      case partialSeq: PartialSequence =>
        val kmers = partialSeq.getBytesToProcess
        val start = partialSeq.getStartValue
        if (kmers == 0) {
          return InputFragment("", 0, "", None)
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
 * A set of input files that can be parsed into [[com.jnpersson.discount.hash.InputFragment]]
 * @param files files to read. A name of the format @list.txt will be parsed as a list of files.
 * @param k length of k-mers
 * @param maxReadLength max length of short sequences
 * @param pairedEnd whether input files are paired-end reads. If so, they are expected to appear in sequence, so that
 *                  the first file is a _1, the second a _2, the third a _1, etc.
 * @param spark the SparkSession
 */
class Inputs(files: Seq[String], k: Int, maxReadLength: Int, pairedEnd: Boolean = false)(implicit spark: SparkSession) {
  protected val conf = new HConfiguration(spark.sparkContext.hadoopConfiguration)
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
   * obtain an appropriate InputReader for a single file.
   */
  def forFile(file: String, file2: Option[String] = None): InputReader[_] = {
    for { f2 <- file2 } {
      println(s"Identified paired end inputs: $file, $f2")
    }
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      new FastqShortInput(file, k, maxReadLength, file2)
    } else {
      //Assume fasta format
      val faiPath = file + ".fai"
      if (HDFSUtil.fileExists(faiPath)) {
        println(s"$faiPath found. Using indexed fasta format for $file")
        new IndexedFastaInput(file, k)
      } else {
        println(s"$faiPath not found. Assuming simple fasta format for $file, max length $maxReadLength")
        new FastaShortInput(file, k, maxReadLength, file2)
      }
    }
  }

  /**
   * Parse all files in this set as InputFragments
   * @param withRC whether to add reverse complement sequences
   * @param withAmbiguous whether to include ambiguous nucleotides. If not, the inputs will be split and only valid
   *                      nucleotides retained.
   * @return
   */
  def getInputFragments(withRC: Boolean, withAmbiguous: Boolean = false,
                        sampleFraction: Option[Double] = None): Dataset[InputFragment] = {
    val readers = if (pairedEnd) {
      if (files.size % 2 != 0) {
        throw new Exception(
          s"For paired end mode, please supply pairs of files (even number). ${files.size} files were supplied")
      }
      expandedFiles.grouped(2).map(pair => forFile(pair(0), Some(pair(1)))).toList
    } else {
      expandedFiles.map(forFile(_, None))
    }
    val fs = readers.map(_.getInputFragments(withRC, withAmbiguous, sampleFraction))
    fs.foldLeft(spark.emptyDataset[InputFragment])(_ union _)
  }

  /**
   * All sequence titles contained in this set of input files
   */
  def getSequenceTitles: Dataset[SeqTitle] = {
    val titles = expandedFiles.map(forFile(_)).map(_.getSequenceTitles)
    titles.foldLeft(spark.emptyDataset[SeqTitle])(_ union _)
  }
}

/**
 * An sequence input converter that reads data from one file (or a pair of paired-end files) using a specific
 * Hadoop format, making the result available as Dataset[InputFragment]
 * @param file the file to read
 * @param k length of k-mers
 * @param spark
 */
abstract class InputReader[R <: AnyRef](file: String, k: Int)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  protected val conf = new HConfiguration(sc.hadoopConfiguration)

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

  protected def loadFile(input: String): RDD[R]
  protected def rdd: RDD[R] = loadFile(file)
  protected[spark] val parser = FragmentParser(k)

  private val validBases = "[ACTGUactgu\n\r]+".r

  /**
   * Split the fragments around unknown or invalid characters.
   */
  private def removeInvalid(data: Dataset[InputFragment]): Dataset[InputFragment] = {
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
   * @return
   */
  protected def getFragments(): Dataset[InputFragment] = {
    val p = parser
    rdd.map(p.toFragment).toDS()
  }

  /**
   * Load sequence fragments from files, optionally adding reverse complements and/or sampling.
   */
  def getInputFragments(withRC: Boolean, withAmbiguous: Boolean,
                        sampleFraction: Option[Double]): Dataset[InputFragment] = {
    val raw = getFragments()
    val valid = if (withAmbiguous) raw else removeInvalid(raw)

    //Note: could possibly push down sampling even deeper
    val sampledValid = sampleFraction match {
      case None => valid
      case Some(f) => valid.sample(f)
    }
    if (withRC) {
      sampledValid.flatMap(r => {
        if (r.nucleotides2.nonEmpty) {
          throw new Exception("RC reading for paired reads is not implemented yet")
        }
        List(r, r.copy(nucleotides = DNAHelpers.reverseComplement(r.nucleotides)))
      })
    } else {
      sampledValid
    }
  }
}

/** Reader for short reads, optionally paired. */
abstract class PairedInputReader[R <: AnyRef](file1: String, k: Int, file2: Option[String])
                                   (implicit spark: SparkSession) extends InputReader[R](file1, k) {
  import spark.sqlContext.implicits._

  override protected def getFragments(): Dataset[InputFragment] = {

    def removeSuffix(f: InputFragment, suffix: String) =
      f.copy(header = f.header.replaceAll(suffix + "$", ""))


    val p = parser
    /* As we currently have no input format that correctly handles paired reads, joining the reads by
       header is the best we can do (and still inexpensive in the big picture).
       Otherwise it is hard to guarantee that they would be paired up correctly.
       We remove the /1 and /2 suffixes from the headers if they are present, as they would break the join.
     */
    file2 match {
      case Some(f2) =>
        val fr1 = rdd.map(x => removeSuffix(p.toFragment(x), "/1")).toDS()
        val fr2 = loadFile(f2).map(x => removeSuffix(p.toFragment(x), "/2")).toDS()
        fr1.joinWith(fr2, fr1("header") === fr2("header")).map(pair =>
          pair._1.copy(nucleotides2 = Some(pair._2.nucleotides)))
      case None => rdd.map(p.toFragment).toDS()
    }
  }
}

/**
 * Input reader for FASTA sequences of a fixed maximum length.
 * Uses [[FASTAshortInputFileFormat]]
 * @param file the file to read
 * @param k length of k-mers
 * @param maxReadLength maximum length of a single read
 * @param file2 second file for paired-end reads
 * @param spark
 */
class FastaShortInput(file: String, k: Int, maxReadLength: Int, file2: Option[String] = None)
                     (implicit spark: SparkSession) extends PairedInputReader[Record](file, k, file2) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength + // sequence data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  protected def loadFile(input: String): RDD[Record] =
    sc.newAPIHadoopFile(input, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS().distinct
}

/**
 * Input reader for FASTQ short reads. Uses [[FASTQInputFileFormat]]
 * @param file the file to read
 * @param k length of k-mers
 * @param maxReadLength maximum length of a single read
 * @param file2 second file for paired-end reads
 * @param spark
 */
class FastqShortInput(file: String, k: Int, maxReadLength: Int, file2: Option[String] = None)
                     (implicit spark: SparkSession) extends PairedInputReader[QRecord](file, k, file2) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength * 2 + // sequence and quality data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  protected def loadFile(input: String): RDD[QRecord] =
    sc.newAPIHadoopFile(input, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS.distinct
}

/**
 * Input reader for FASTA files containing potentially long sequences, with a .fai index
 * FAI indexes can be created with tools such as seqkit.
 * Uses [[IndexedFastaFormat]]
 *
 * @param file the file to read
 * @param k length of k-mers
 * @param spark
 */
class IndexedFastaInput(file: String, k: Int)(implicit spark: SparkSession)
  extends InputReader[PartialSequence](file, k) {
  import spark.sqlContext.implicits._

  protected def loadFile(input: String): RDD[PartialSequence] =
    sc.newAPIHadoopFile(input, classOf[IndexedFastaFormat], classOf[Text], classOf[PartialSequence], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS.distinct
}
