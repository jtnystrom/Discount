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

package com.jnpersson.discount

/** Provides classes and routines for running on Apache Spark.
 * The main entry point is the [[Discount]] class. Once configured, it can be used to generate other classes of interest,
 * such as [[GroupedSegments]] and [[CountedKmers]].*/
package object spark {

  /** Defines a strategy for counting k-mers in Spark. */
  sealed trait CountMethod {
    /** Whether k-mer orientation should be normalized in the final result (by adding reverse complements
     * at some stage and filtering out non-canonical k-mers) */
    def normalize: Boolean

    /** Whether reverse complement data should be added at the input stage */
    def addRCToMainData: Boolean = normalize
  }

  /** Pregrouped counting: groups and counts identical super-mers before counting k-mers.
   * Faster for datasets with high redundancy. */
  case class Pregrouped(normalize: Boolean) extends CountMethod {
    override def addRCToMainData: Boolean = false
  }

  /** Non-pregrouped: counts k-mers immediately. Faster for datasets with low redundancy. */
  case class Simple(normalize: Boolean) extends CountMethod

}

package spark.minimizers {
  /**
   * A method for obtaining a set of minimizers for given values of k and m.
   * Except for the case of All, the sets obtained should be universal hitting sets (UHSs).
   * Note that these methods does not specify the ordering of the minimizers.
   */
  sealed trait Source

  /**
   * A file, or a directory containing multiple files with names like minimizers_{k}_{m}.txt,
   * in which case the best file will be selected.
   * @param path
   */
  final case class Path(path: String) extends Source

  /**
   * Bundled minimizers on the classpath (may not be available for all values of k).
   */
  case object Bundled extends Source

  /**
   * Use all m-mers as minimizers. Can be auto-generated for any m.
   */
  case object All extends Source
}