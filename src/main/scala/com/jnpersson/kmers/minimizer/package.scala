/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import org.apache.spark.sql.SparkSession

/** A module for finding minimizers and splitting nucleotide sequences into super-mers.
 */
package object minimizer {
  /** The type of a bucket ID (derived from minimizer). Buckets can be used
   * to group super-mers.
   */
  type BucketId = Long

  /** For [[RandomXOR]] ordering */
  //from mmscanner.h in kraken2
  val DEFAULT_TOGGLE_MASK = 0xe37e28c4271b5a2dL

  /** An ordering of a minimizer set */
  sealed trait MinimizerOrdering

  /** Ordering by frequency (rare to common)
   * @param bySequence Whether to count distinct sequences that the minimizers occur in,
   * instead of total occurrences
   */
  final case class Frequency(bySequence: Boolean = false) extends MinimizerOrdering

  /** A user-specified ordering */
  case object Given extends MinimizerOrdering

  /** Lexicographic (alphabetical) ordering */
  case object Lexicographic extends MinimizerOrdering

  /** Ordering obtained by XORing with a mask
   * @param mask The XOR mask
   * @param canonical Whether to canonicalize the orientation (forward/reverse) of minimizers */
  final case class XORMask(mask: Long = DEFAULT_TOGGLE_MASK,
                           canonical: Boolean = false) extends MinimizerOrdering

  /** A derived ordering that maps every minimizer to its canonical
   * (forward) orientation */
  final case class Canonical(inner: MinimizerOrdering) extends MinimizerOrdering

  /** Orientations of k-mers. */
  sealed trait Orientation

  /** Forward orientated (canonical) k-mers, i.e. those that are lexicographically prior to their reverse complement.
   * During normalized k-mer counting, k-mers are flipped to the canonical orientation if necessary.
   */
  case object Forward extends Orientation

  /** Both forward and reverse oriented k-mers */
  case object Unchanged extends Orientation


  /**
   * A method for obtaining a set of minimizers for given values of k and m.
   * The sets obtained should be universal hitting sets (UHSs), or otherwise guaranteed to hit every
   * k-mer in practice.
   * Only m <= 15 can be loaded in this way.
   */
  trait MinimizerSource {

    /** The maximum possible number of minimizers for the given m */
    def theoreticalMax(m: Int): Long = 1L << (m * 2) // 4 ^ m

    /** Obtain the encoded minimizers in order */
    def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int]

    /** Convert a MinimizerPriorities to a MinSplitter using this source */
    def toSplitter(priorities: MinimizerPriorities, k: Int)(implicit spark: SparkSession): MinSplitter[_ <: MinimizerPriorities] =
      MinSplitter(priorities, k)
  }

  /**
   * Use all m-mers as minimizers. Can be auto-generated for any m.
   * The initial ordering is lexicographic.
   */
  case object All extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] =
      Array.range(0, 1 << (2 * m))
  }

  /** Programmatially generated minimizers. Will be used in the given order
   * if minimizerOrder = [[Given]] is used
   * @param byPriority the minimizers in the given order.
   * */
  final case class Generated(byPriority: Array[Int]) extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] =
      byPriority
  }
}
