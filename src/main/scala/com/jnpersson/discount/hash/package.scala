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

package com.jnpersson.discount

/** Provides classes for hashing k-mers and nucleotide sequences. Hashing is done by identifying minimizers.
 * Hashing all k-mers in a sequence thus corresponds to splitting the sequence into
 * super-mers of length >= k (super k-mers) where all k-mers share the same minimizer.
 */
package object hash {
  /** Type of a compacted hash (minimizer) */
  type BucketId = Long

  /** For [[RandomXOR]] ordering */
  //from mmscanner.h in kraken2
  val DEFAULT_TOGGLE_MASK = 0xe37e28c4271b5a2dL
}
