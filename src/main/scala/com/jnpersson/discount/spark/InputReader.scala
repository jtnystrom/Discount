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
import com.jnpersson.discount.{NTSeq, SeqTitle}
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

  private def toFragments(record: Record, k: Int): Iterator[InputFragment] =
    splitFragment(InputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getValue), k,
      FRAGMENT_MAX_SIZE)

  private def toFragments(record: QRecord, k: Int): Iterator[InputFragment] =
    splitFragment(InputFragment(record.getKey.split(" ")(0), FIRST_LOCATION, record.getValue), k,
      FRAGMENT_MAX_SIZE)

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
    splitFragment(InputFragment(key, FIRST_LOCATION, nts), k, FRAGMENT_MAX_SIZE)
  }

  /**
   * Split a long fragment into shorter fragments of a controlled maximum size.
   * The resulting fragments will overlap by (k-1) nucleotides
   * @param fragment fragment to split
   * @param k k
   * @return
   */
  private[spark] def splitFragment(fragment: InputFragment, k: Int, maxsize: Int): Iterator[InputFragment] = {
    val nonNewline = s"[^\n]{1,$maxsize}".r
    val allMatches = nonNewline.findAllMatchIn(fragment.nucleotides).buffered
    var consumed = 0 //number of valid NT characters we have seen

    new Iterator[InputFragment] {
      override def hasNext: Boolean = allMatches.hasNext

      override def next(): InputFragment = {
        val start = fragment.location + consumed
        val buffer = new StringBuilder
        while (buffer.length < maxsize && allMatches.hasNext) {
          buffer.append(allMatches.next.toString())
        }
        consumed += buffer.length

        //we need to ensure a (k-1) overlap part without newlines in order not to miss any k-mers when we split
        if (allMatches.hasNext) {
          buffer.append(allMatches.head.toString.take(k - 1))
        }
        InputFragment(fragment.header, start, buffer.toString());
      }
    }
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
