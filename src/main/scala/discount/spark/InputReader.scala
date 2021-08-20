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

package discount.spark

import fastdoop._
import discount.{NTSeq, SequenceID}
import discount.util.DNAHelpers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Routines for reading input data using fastdoop.
 * @param spark
 * @param k
 */
class InputReader(maxReadLength: Int, k: Int, multilineFasta: Boolean)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  private val conf = new Configuration(sc.hadoopConfiguration)

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

  //Estimate for the largest string we need to read, plus some extra space.
  private val bufsiz = maxReadLength * 2 + // sequence data and quality (fastq)
    1000 //ID string and separator characters

  //Fastdoop parameter
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  /* Constrain the split sizes for input files (increasing the number of splits).
   * For longer sequences in "short read files", such as the NCBI bacterial sequences refseq library,
   * large splits will cause a lot of memory pressure. This helps control the effect.
   */
//  conf.set("mapred.max.split.size", (4 * 1024 * 1024).toString)

  private def sampleRDD[A](data: RDD[A], fraction: Option[Double]): RDD[A] = {
    fraction match {
      case Some(s) => data.sample(false, s)
      case _ => data
    }
  }

  private def ingestFasta(data: RDD[NTSeq]): RDD[NTSeq] = {
    if (multilineFasta) {
      data.map(_.replaceAll("\n", ""))
    } else {
      data
    }
  }

  private def shortReadsWarning(): Unit = {
    println("(This input format is only for short reads. If you are reading long sequences, consider using" +
      " --long and/or --multiline.)")
  }

  /**
   * Read short read sequence data only from the input file.
   * @param file
   * @return
   */
  private def getShortReads(file: String, sample: Option[Double]): RDD[NTSeq] = {
    shortReadsWarning()
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        conf)
      sampleRDD(ss, sample).map(_._2.getValue)
    } else {
      println(s"Assuming fasta short read format for $file (multiline: $multilineFasta), max length $maxReadLength")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        conf)
      ingestFasta(sampleRDD(ss, sample).map(x => x._2.getValue))
    }
  }

  /**
   * Read short sequence data together with sequence IDs.
   * @param file
   * @return
   */
  private def getShortReadsWithID(file: String): RDD[(SequenceID, NTSeq)] = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file (with ID), max length $maxReadLength")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        conf)
      ss.map(r => (r._2.getKey.split(" ")(0), r._2.getValue))
    } else {
      println(s"Assuming fasta short read format for $file (with ID) (multiline: $multilineFasta), max length $maxReadLength")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        conf)
      if (multilineFasta) {
        ss.map(r => (r._2.getKey.split(" ")(0), r._2.getValue.replaceAll("\n", "")))
      } else {
        ss.map(r => (r._2.getKey.split(" ")(0), r._2.getValue))
      }
    }
  }

  /**
   * Read a single long sequence.
   * @param file
   * @return
   */
  def getLongSequence(file: String, sample: Option[Double]): RDD[NTSeq] = {
    println(s"Assuming fasta format (long sequences) for $file (multiline: $multilineFasta)")
    println("(This input format is only for long sequences. If you are reading short reads, you should not use --long)")
    val ss = sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      conf)
    ingestFasta(sampleRDD(ss, sample).map(_._2.getValue))
  }

  private val degenerateAndUnknown = "[^ACTGUactgu]+"

  /**
   * Load sequences from files, optionally adding reverse complements and/or sampling.
   */
  def getReadsFromFiles(fileSpec: String, withRC: Boolean,
                        sample: Option[Double] = None,
                        longSequence: Boolean = false): Dataset[NTSeq] = {
    val raw = if(longSequence)
      getLongSequence(fileSpec, sample).toDS
    else
      getShortReads(fileSpec, sample).toDS

    val degen = this.degenerateAndUnknown
    val valid = raw.flatMap(r => r.split(degen))

    if (withRC) {
      valid.flatMap(r => {
        Seq(r, DNAHelpers.reverseComplement(r))
      })
    } else {
      valid
    }
  }

  /**
   * Load reads with their IDs from DNA files.
   * @param fileSpec
   * @param withRC
   * @param longSequence
   * @return
   */
  def getReadsFromFilesWithID(fileSpec: String, withRC: Boolean,
                              longSequence: Boolean = false): Dataset[(SequenceID, NTSeq)] = {
    val raw = if(longSequence)
      ???
    else
      getShortReadsWithID(fileSpec).toDS

    val degen = this.degenerateAndUnknown
    val valid = raw.flatMap(r => r._2.split(degen).map(s => (r._1, s)))

    if (withRC) {
      valid.flatMap(r => {
        Seq(r, (r._1, DNAHelpers.reverseComplement(r._2)))
      })
    } else {
      valid
    }
  }
}