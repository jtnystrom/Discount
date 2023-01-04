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

  /** Ordering by frequency (rare to common) */
  case object Frequency extends MinimizerOrdering

  /** Random ordering */
  case object Random extends MinimizerOrdering

  /** A user-specified ordering */
  case object Given extends MinimizerOrdering

  /** Lexicographic (alphabetical) ordering */
  case object Lexicographic extends MinimizerOrdering

  /** Ordering by minimizer signature, as in KMC2/3 */
  case object Signature extends MinimizerOrdering

}
