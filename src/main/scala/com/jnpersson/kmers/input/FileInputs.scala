/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers.input

import com.jnpersson.fastdoop.{IndexedFastaFormat, PartialSequence}
import com.jnpersson.kmers.{HDFSUtil, SeqLocation, SeqTitle}
import com.jnpersson.kmers.minimizer.InputFragment
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, element_at, lit, monotonically_increasing_id, slice, substring}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.parallel.immutable.ParVector
import scala.collection.compat._

/**
 * A set of input files that can be parsed into [[InputFragment]]
 *
 * @param files files to read. A name of the format @list.txt will be parsed as a list of files.
 * @param k length of k-mers
 * @param inputGrouping whether input files are paired-end reads. If so, they are expected to appear in sequence, so that
 *                  the first file is a _1, the second a _2, the third a _1, etc.
 * @param spark the SparkSession
 */
class FileInputs(val files: Seq[String], k: Int, inputGrouping: InputGrouping = Ungrouped)(implicit spark: SparkSession) {
  protected val conf = new HConfiguration(spark.sparkContext.hadoopConfiguration)
  import spark.sqlContext.implicits._

  /** Clone this Inputs with a different value of k. */
  def withK(newK: Int): FileInputs =
    new FileInputs(files, newK, inputGrouping)

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
  def forFile(file: String): InputReader = {
    val lower = file.toLowerCase
    val faiPath = file + ".fai"

    if (lower.endsWith("fq") || lower.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      new FastqTextInput(file)
    } else if (lower.endsWith(".fq.gz") || lower.endsWith(".fastq.gz") ||
        lower.endsWith(".fq.bz2") || lower.endsWith(".fastq.bz2")) {
      println(s"Assuming compressed fastq format for $file")
      new FastqTextInput(file)
    } else if (HDFSUtil.fileExists(faiPath)) {
      println(s"$faiPath found. Using indexed fasta format for $file")
      new IndexedFastaInput(file, k)
    } else if (lower.endsWith(".gz") || lower.endsWith(".bz2") ) {
      println(s"Assuming compressed fasta format for $file")
      new FastaTextInput(file)
    } else {
      println(s"$faiPath not found. Assuming simple fasta format for $file")
      new FastaTextInput(file)
    }
  }

  /**
   * By looking at the file name and checking for the presence of a .fai file in the case of fasta,
   * obtain an appropriate InputReader for a single file.
   * Read the files as paired-end reads if the second file was supplied.
   */
  def forPair(file: String, file2: String): PairedInputReader = {
    println(s"Identified paired end inputs: $file, $file2")
    new PairedInputReader(forFile(file), forFile(file2))
  }

  /**
   * Parse all files in this set as InputFragments
   * @param withAmbiguous whether to include ambiguous nucleotides. If not, the inputs will be split and only valid
   *                      nucleotides retained.
   * @param sampleFraction the fraction to sample, if any.
   */
  def getInputFragments(withAmbiguous: Boolean = false, sampleFraction: Option[Double] = None): Dataset[InputFragment] = {
    val readers = inputGrouping match {
      case PairedEnd =>
        if (files.size % 2 != 0) {
          throw new Exception(
            s"For paired end mode, please supply pairs of files (even number). ${files.size} files were supplied")
        }
        expandedFiles.grouped(2).map(pair => forPair(pair(0), pair(1))).toList
      case _ =>
        expandedFiles.map(forFile)
    }
    val fs = readers.to(ParVector).map(_.getInputFragments(withAmbiguous, sampleFraction)).seq
    spark.sparkContext.union(fs.map(_.rdd)).toDS()
  }

  /**
   * All sequence titles contained in this set of input files
   */
  def getSequenceTitles: Dataset[SeqTitle] = {
    val titles = expandedFiles.map(forFile(_)).map(_.getSequenceTitles)
    spark.sparkContext.union(titles.map(_.rdd)).toDS()
  }
}

object HadoopInputReader {
  val FIRST_LOCATION = 1

  def makeInputFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                        start: Int, end: Int): InputFragment = {
    val nucleotides = new String(buffer, start, end - start + 1)
    InputFragment(header, location, nucleotides, None)
  }
}

/**
 * A sequence input converter that reads data from one file using a specific
 * Hadoop format, making the result available as Dataset[InputFragment]
 * @param file the file to read
 */
abstract class HadoopInputReader[R <: AnyRef](file: String)(implicit spark: SparkSession) extends InputReader {
  protected val conf = new HConfiguration(sc.hadoopConfiguration)

  protected def loadFile(input: String): RDD[R]
  protected def rdd: RDD[R] = loadFile(file)

  protected[input] def getFragments(): Dataset[InputFragment]
}

/**
 * Reader for fasta records that are potentially multiline, but small enough to fit into a single string.
 * Huge sequences are best processed with the [[IndexedFastaFormat]] instead (.fna files)
 * Supports compression via Spark's text reader.
 */
class FastaTextInput(file: String)(implicit spark: SparkSession) extends HadoopInputReader[Array[String]](file) {
  import spark.sqlContext.implicits._
  import HadoopInputReader._

  protected def loadFile(input: String): RDD[Array[String]] = {
    import spark.implicits._
    spark.read.option("lineSep", ">").text(file).as[String]. //allows multi-line fasta records to be separated cleanly
      flatMap(x => {
        val spl = x.split("[\n\r]+")
        if (spl.length >= 2) {
          Some(spl)
        } else {
          None
        }
      }).rdd
  }

  protected[input] def getFragments(): Dataset[InputFragment] =
    rdd.map(x => {
      val headerLine = x(0)
      val id = headerLine.split(" ")(0)
      val nucleotides = x.drop(1).mkString("")
      InputFragment(id, FIRST_LOCATION, nucleotides, None)
    }).toDS()

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(x => x(0)).toDS()
}

/**
 * Reader for fastq records. Supports compression via Spark's text input layer.
 */
class FastqTextInput(file: String)(implicit spark: SparkSession) extends HadoopInputReader[Array[String]](file) {
  import spark.sqlContext.implicits._
  import HadoopInputReader._

  protected def loadFile(input: String): RDD[Array[String]] = {
    import spark.implicits._

    spark.read.text(file).
      withColumn("file", lit(file)).
      withColumn("rowId", monotonically_increasing_id()).
      withColumn("values", //group every 4 rows and give them the same recId
        collect_list($"value").over(Window.partitionBy("file").orderBy("rowId").rowsBetween(Window.currentRow, 3))
      ).
      where(
        //In a valid FASTQ record, line 1/4 begins with @ and line 3/4 begins with +.
        //The characters @ and + may sometimes also appear in quality scores.
        //Checking both at lines 1 and 3 is a robust way to identify the right position of the 4-line window even
        //in the presence of such confounding characters.
        (substring(element_at($"values", 1), 1, 1) === "@").and(
          substring(element_at($"values", 3), 1, 1) === "+")
      ).
      select(slice($"values", 1, 2)).as[Array[String]] //Currently preserves only the header and the nucleotide string
  }.rdd

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(x => x(0)).toDS()

  protected[input] def getFragments(): Dataset[InputFragment] =
    rdd.map(ar => {
      val id = ar(0).split(" ")(0).substring(1) //remove leading @
      val nucleotides = ar(1)
      InputFragment(id, FIRST_LOCATION, nucleotides, None)
    }).toDS()
}

/**
 * Input reader for FASTA files containing potentially long sequences, with a .fai index
 * FAI indexes can be created with tools such as seqkit.
 * Uses [[IndexedFastaFormat]]
 *
 * @param file the file to read
 * @param k length of k-mers. Needed for this input format as k-mers will cross boundaries between file splits
 */
class IndexedFastaInput(file: String, k: Int)(implicit spark: SparkSession)
  extends HadoopInputReader[PartialSequence](file) {

  import spark.sqlContext.implicits._
  import HadoopInputReader._

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

  protected def loadFile(input: String): RDD[PartialSequence] =
    sc.newAPIHadoopFile(input, classOf[IndexedFastaFormat], classOf[Text], classOf[PartialSequence], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS().distinct()

  protected[input] def getFragments(): Dataset[InputFragment] = {
    val k = this.k

    rdd.map(partialSeq => {
      val kmers = partialSeq.getBytesToProcess
      val start = partialSeq.getStartValue
      if (kmers == 0) {
        InputFragment("", 0, "", None)
      } else {

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
    }).toDS()
  }
}
