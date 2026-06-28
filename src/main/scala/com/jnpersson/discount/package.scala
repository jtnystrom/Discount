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

package com.jnpersson

/**
 * Root package for this application.
 */
package object discount {

  /** Type of nucleotide sequences in human-readable form. */
  type NTSeq = String

  /** Type of Sequence titles/headers (as read from fasta/fastq files) */
  type SeqTitle = String

  /** Type of Sequence IDs */
  type SeqID = Int

  /** Type of locations on sequences */
  type SeqLocation = Long

  /** Internal type of abundance counts for k-mers. Even though this is is a Long,
   * some algorithms use 32-bit values, so overall only 32-bit counters are currently supported,
   * bounded by the two values below. */
  type Abundance = Long

  /** Minimum value for abundance */
  def abundanceMin: Int = Int.MinValue

  /** Maximum value for abundance */
  def abundanceMax: Int = Int.MaxValue

  /** A type of ordering of a minimizer set */
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
  final case class XORMask(mask: Long = hash.DEFAULT_TOGGLE_MASK,
                     canonical: Boolean = false) extends MinimizerOrdering

  /** Orientations of k-mers. */
  sealed trait Orientation

  /** Forward orientated k-mers, i.e. those that are lexicographically prior to their reverse complement.
   * During normalized k-mer counting, only forward orientation k-mers are kept. */
  case object ForwardOnly extends Orientation

  /** Both forward and reverse oriented k-mers */
  case object Both extends Orientation
}
