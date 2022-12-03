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

import com.jnpersson.discount.hash.{BundledMinimizers, MinSplitter, MinTable, MinimizerPriorities, RandomXOR}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/** Provides classes and routines for running on Apache Spark.
 * The main entry point is the [[Discount]] class. Once configured, it can be used to generate other classes of interest,
 * such as [[GroupedSegments]] and [[CountedKmers]].*/
package object spark {

  type AnyMinSplitter = MinSplitter[MinimizerPriorities]

  object Helpers {
    private var encoders = Map[Class[_], Encoder[_]]()

    def registerEncoder(cls: Class[_], enc: Encoder[_]): Unit = synchronized {
      println(s"Register $cls")
      encoders += cls -> enc
    }

    def encoder[S <: MinSplitter[_]](spl: S): Encoder[S] = synchronized {
      spl.priorities match {
        case _: MinTable => Encoders.product[(MinSplitter[MinTable])].asInstanceOf[Encoder[S]]
        case _: RandomXOR => Encoders.product[(MinSplitter[RandomXOR])].asInstanceOf[Encoder[S]]
        case _ => encoders(spl.priorities.getClass).asInstanceOf[Encoder[S]]
      }
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
  trait MinimizerSource {
    def theoreticalMax(m: Int) = 1L << (m * 2) // 4 ^ m

    def load(k: Int, m: Int)(implicit spark: SparkSession): Seq[NTSeq]

    def finish(priorities: MinimizerPriorities, k: Int)(implicit spark: SparkSession): MinSplitter[_ <: MinimizerPriorities] =
      MinSplitter(priorities, k)
  }

  /**
   * A file, or a directory containing multiple files with names like minimizers_{k}_{m}.txt,
   * in which case the best file will be selected. These files may specify an ordering.
   * @param path
   */
  final case class Path(path: String) extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Seq[NTSeq] = {
      val s = new Sampling()
      val use = s.readMotifList(path)
      println(s"${use.length}/${theoreticalMax(m)} $m-mers will become minimizers (loaded from $path)")
      use
    }
  }

  /**
   * Bundled minimizers on the classpath (only available for some values of k and m).
   * May specify an undefined ordering.
   */
  case object Bundled extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Seq[NTSeq] = {
      BundledMinimizers.getMinimizers(k, m) match {
        case Some(internalMinimizers) =>
          println (s"${internalMinimizers.length}/${theoreticalMax(m)} $m-mers will become minimizers(loaded from classpath)")
          internalMinimizers.toSeq
        case _ =>
          throw new Exception(s"No classpath minimizers found for k=$k, m=$m. Please specify minimizers with --minimizers\n" +
            "or --allMinimizers for all m-mers.")
      }
    }
  }

  /**
   * Use all m-mers as minimizers. Can be auto-generated for any m.
   * The initial ordering is lexicographic.
   */
  case object All extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Seq[NTSeq] = {
      MinTable.ofLength(m).byPriority
    }
  }
}
