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

package com.jnpersson.kmers

import org.apache.spark.sql.SparkSession

/** Provides classes for hashing k-mers and nucleotide sequences. Hashing is done by identifying minimizers.
 * Hashing all k-mers in a sequence thus corresponds to splitting the sequence into
 * super-mers of length >= k (super k-mers) where all k-mers share the same minimizer.
 */
package object minimizer {
  /** The type of a compacted hash (minimizer) */
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

  /** Ordering by minimizer signature, as in KMC2/3 */
  case object Signature extends MinimizerOrdering

  /** Ordering obtained by XORing with a mask
   * @param mask The XOR mask
   * @param canonical Whether to canonicalize the orientation (forward/reverse) of minimizers */
  final case class XORMask(mask: Long = DEFAULT_TOGGLE_MASK,
                           canonical: Boolean = false) extends MinimizerOrdering

  /** Orientations of k-mers. */
  sealed trait Orientation

  /** Forward orientated k-mers, i.e. those that are lexicographically prior to their reverse complement.
   * During normalized k-mer counting, only forward orientation k-mers are kept. */
  case object ForwardOnly extends Orientation

  /** Both forward and reverse oriented k-mers */
  case object Both extends Orientation


  /**
   * A method for obtaining a set of minimizers for given values of k and m.
   * The sets obtained should be universal hitting sets (UHSs), or otherwise guaranteed to hit every
   * k-mer in practice.
   * Only m <= 15 can be loaded in this way.
   */
  trait MinimizerSource {
    def theoreticalMax(m: Int): SeqLocation = 1L << (m * 2) // 4 ^ m

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
   * if minimizerOrder = [[Given]] is used  */
  final case class Generated(byPriority: Array[Int]) extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] =
      byPriority
  }
}
