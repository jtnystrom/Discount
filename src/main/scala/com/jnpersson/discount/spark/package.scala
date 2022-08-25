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

import com.jnpersson.discount.hash.{MinSplitter, MinimizerPriorities, MotifSpace, RandomXOR}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

/** Provides classes and routines for running on Apache Spark.
 * The main entry point is the [[Discount]] class. Once configured, it can be used to generate other classes of interest,
 * such as [[GroupedSegments]] and [[CountedKmers]].*/
package object spark {

  type AnyMinSplitter = MinSplitter[MinimizerPriorities]

  object Helpers {
    def encoder[S <: MinSplitter[_]](spl: S): Encoder[S] =
      spl.priorities match {
        case ms: MotifSpace => Encoders.product[(MinSplitter[MotifSpace])].asInstanceOf[Encoder[S]]
        case rx: RandomXOR => Encoders.product[(MinSplitter[RandomXOR])].asInstanceOf[Encoder[S]]
      }
  }

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
    override def toString = s"Pregrouped (normalize: $normalize)"
  }

  /** Non-pregrouped: counts k-mers immediately. Faster for datasets with low redundancy. */
  case class Simple(normalize: Boolean) extends CountMethod {
    override def toString = s"Simple (normalize: $normalize)"
  }

  /**
   * A method for obtaining a set of minimizers for given values of k and m.
   * Except for the case of All, the sets obtained should be universal hitting sets (UHSs).
   */
  sealed trait MinimizerSource

  /**
   * A file, or a directory containing multiple files with names like minimizers_{k}_{m}.txt,
   * in which case the best file will be selected. These files may specify an ordering.
   * @param path
   */
  final case class Path(path: String) extends MinimizerSource

  /**
   * Bundled minimizers on the classpath (only available for some values of k and m).
   * May specify an undefined ordering.
   */
  case object Bundled extends MinimizerSource

  /**
   * Use all m-mers as minimizers. Can be auto-generated for any m.
   * The initial ordering is lexicographic.
   */
  case object All extends MinimizerSource
}
