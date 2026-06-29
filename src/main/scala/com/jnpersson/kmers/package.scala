/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson

import com.jnpersson.kmers.minimizer._

/**
 * This package contains routines for processing k-mers, super-mers and minimizers.
 */
package object kmers {

  /** Type of nucleotide sequences in human-readable form. */
  type NTSeq = String

  /** Type of Sequence titles/headers (as read from fasta/fastq files) */
  type SeqTitle = String

  /** Type of Sequence IDs */
  type SeqID = Int

  /** Type of locations on sequences */
  type SeqLocation = Long

  /** Internal type of abundance counts for k-mers. Even though this is a Long,
   * some algorithms use 32-bit values, so overall only 32-bit counters are currently supported,
   * bounded by the two values below. */
  type Abundance = Long

  /** Minimum value for abundance */
  def abundanceMin: Int = Int.MinValue

  /** Maximum value for abundance */
  def abundanceMax: Int = Int.MaxValue

  type AnyMinSplitter = MinSplitter[MinimizerPriorities]

  object Helpers {
    def randomTableName: String = {
      val rnd = scala.util.Random.nextLong()
      val useRnd = if (rnd < 0) - rnd else rnd
      s"discount_$useRnd"
    }

    private val formatsById = Map[String, SplitterFormat[_]](
      "standard" -> new StandardFormat(),
      "randomXOR" -> new RandomXORFormat(),
      "extended" -> new ExtendedFormat())

    private val formatsByCls = Map[Class[_], SplitterFormat[_]](
      classOf[MinTable] -> new StandardFormat(),
      classOf[RandomXOR] -> new RandomXORFormat(),
      classOf[ExtendedTable] -> new ExtendedFormat())

    /** Obtain a previously registered SplitterFormat by id */
    def getFormat(id: String): SplitterFormat[_] = synchronized {
      formatsById.getOrElse(id, throw new Exception(s"No such format $id"))
    }

    /** Obtain a previously registered SplitterFormat by class */
    def getFormat[P <: MinimizerPriorities](cls: Class[_ <: P]): SplitterFormat[P] = synchronized {
      formatsByCls.getOrElse(cls, throw new Exception(s"No format for class $cls")).asInstanceOf[SplitterFormat[P]]
    }

    /** Format a fraction as a percentage string */
    def formatPerc(d: Double): String = "%.2f%%".format(d * 100)
  }

}
