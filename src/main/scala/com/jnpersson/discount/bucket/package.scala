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

/** Provides routines related to buckets/bins of k-mers. */
package object bucket {
  //a k-mer tag (annotation of some kind)
  type Tag = Int


  /**
   * k-mer combination (reduction) rules for combining indexes.
   * Most of these support both intersection and union. An intersection is an operation that requires
   * the k-mer to be present in every input index, or it will not be present in the output. A union may preserve
   * the k-mer even if it is present in only one input index.
   * Except for the case of the union Sum reduction, indexes must be compacted prior to reduction, that is, each
   * k-mer must occur in each index with a nonzero value only once.
   *
   * These rules were inspired by the design of KMC3: https://github.com/refresh-bio/KMC
   */
  sealed trait Rule extends Serializable

  object Rule {

    /** Convert a Long to Int without overflowing Int.MaxValue */
    def cappedLongToInt(x: Long): Int =
      if (x > Int.MaxValue) Int.MaxValue else x.toInt

    /** Add k-mer counts together */
    object Sum extends Rule

    /** Select the maximum value */
    object Max extends Rule

    /** Select the minimum value */
    object Min extends Rule

    /** Select the first value */
    object Left extends Rule

    /** Select the second value */
    object Right extends Rule

    /** Subtract k-mer counts A-B, preserving positive results. */
    object CountersSubtract extends Rule

    /** Preserve only those k-mers that were present in A but absent in B (weaker version of subtract)
     * This does not support intersection, since the result would always be empty. */
    object KmersSubtract extends Rule

  }
}
