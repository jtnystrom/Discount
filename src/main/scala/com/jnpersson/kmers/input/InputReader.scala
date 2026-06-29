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

import com.jnpersson.kmers.minimizer.InputFragment
import com.jnpersson.kmers.util.DNAHelpers
import com.jnpersson.kmers.{HDFSUtil, SeqLocation, SeqTitle}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.postfixOps


sealed trait InputGrouping
/** Single reads or genomes */
case object Ungrouped extends InputGrouping
/** Paired-end reads */
case object PairedEnd extends InputGrouping

object InputReader {
  /** Add reverse complement sequences to input fragments.
   * Returns the original fragments plus their reverse complement.
   */
  def addRCFragments(fs: Dataset[InputFragment])(implicit spark: SparkSession): Dataset[InputFragment] = {
    import spark.sqlContext.implicits._
    fs.flatMap(r =>
      List(r, r.copy(
        nucleotides = DNAHelpers.reverseComplement(r.nucleotides),
        nucleotides2 = r.nucleotides2.map(n2 => DNAHelpers.reverseComplement(n2))
      ))
    )
  }
}

/** A reader for InputFragments from some source.
 * Optionally removes invalid sequences.
 * Optionally samples the input.
 */
abstract class InputReader(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  //Multiline DNA sequences may contain intermittent newlines but should
  //still be treated as a single valid match.

  private val validBases = "[ACTGUactgu][ACTGUactgu\n\r]*".r

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
   * Read sequence data as fragments from the input source
   * @return
   */
  protected[input] def getFragments(): Dataset[InputFragment]

  /**
   * Load sequence fragments from the source
   * @param withAmbiguous Whether to remove ambiguous characters and whitespace
   * @param The fraction to sample, if any
   */
  def getInputFragments(withAmbiguous: Boolean,
                        sampleFraction: Option[Double]): Dataset[InputFragment] = {
    val raw = getFragments()
    val valid = if (withAmbiguous) raw else removeInvalid(raw)

    //Note: could possibly push down sampling even deeper
    sampleFraction match {
      case None => valid
      case Some(f) => valid.sample(f)
    }
  }
}

/** Transforms two input readers (R1 and R2 lanes) into a reader of paired-end reads */
class PairedInputReader(lhs: InputReader, rhs: InputReader)(implicit spark: SparkSession) extends InputReader {
  import spark.sqlContext.implicits._
  import PairedInputReader._

  protected[input] def getFragments(): Dataset[InputFragment] = {
    /* As we currently have no input format that correctly handles paired reads, joining the reads by
          header is the best we can do (and still inexpensive in the big picture).
          Otherwise, it is hard to guarantee that they would be paired up correctly.
          We remove the /1 and /2 suffixes from the headers if they are present, as they would break the join.
        */
    val fr1 = lhs.getFragments().map(x => removeSuffix(x, "/1"))
    val fr2 = rhs.getFragments().map(x => removeSuffix(x, "/2"))
    fr1.joinWith(fr2, fr1("header") === fr2("header")).map(pair =>
      pair._1.copy(nucleotides2 = Some(pair._2.nucleotides)))
  }

  override def getSequenceTitles: Dataset[SeqTitle] =
    lhs.getSequenceTitles.map(header => removeSuffix(header, "/1"))
}

object PairedInputReader {
  def removeSuffix(header: String, suffix: String): String =
    header.replaceAll(suffix + "$", "")

  def removeSuffix(f: InputFragment, suffix: String): InputFragment =
    f.copy(header = removeSuffix(f.header, suffix))
}
