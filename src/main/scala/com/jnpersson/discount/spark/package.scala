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

import com.jnpersson.discount.hash._
import com.jnpersson.discount.util.NTBitArray
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/** Provides classes and routines for running on Apache Spark.
 * The main entry point is the [[Discount]] class. Once configured, it can be used to generate other classes of interest,
 * such as [[GroupedSegments]] and [[CountedKmers]].*/
package object spark {

  type AnyMinSplitter = MinSplitter[MinimizerPriorities]

  object Helpers {
    private var encoders = Map.empty[Class[_], Encoder[_]]

    /** Register a Spark Encoder for a given class */
    def registerEncoder(cls: Class[_], enc: Encoder[_]): Unit = synchronized {
      println(s"Register $cls")
      encoders += cls -> enc
    }

    /** Obtain a known or previously registered Spark Encoder for a given class */
    def encoder[S <: MinSplitter[_]](spl: S): Encoder[S] = synchronized {
      spl.priorities match {
        case _: MinTable => Encoders.product[MinSplitter[MinTable]].asInstanceOf[Encoder[S]]
        case _: RandomXOR => Encoders.product[MinSplitter[RandomXOR]].asInstanceOf[Encoder[S]]
        case _: ExtendedTable => Encoders.product[MinSplitter[ExtendedTable]].asInstanceOf[Encoder[S]]
        case _ => encoders(spl.priorities.getClass).asInstanceOf[Encoder[S]]
      }
    }

    private var formatsById = Map[String, SplitterFormat[_]](
      "standard" -> new StandardFormat(),
      "randomXOR" -> new RandomXORFormat(),
      "extended" -> new ExtendedFormat())

    private var formatsByCls = Map[Class[_], SplitterFormat[_]](
      classOf[MinTable] -> new StandardFormat(),
      classOf[RandomXOR] -> new RandomXORFormat(),
      classOf[ExtendedTable] -> new ExtendedFormat())

    /** Register a SplitterFormat */
    def registerFormat[P <: MinimizerPriorities](cls: Class[P], format: SplitterFormat[P]): Unit = synchronized {
      formatsById += format.id -> format
      formatsByCls += cls -> format
    }

    /** Obtain a previously registered SplitterFormat by id */
    def getFormat(id: String): SplitterFormat[_] = synchronized {
      formatsById.getOrElse(id, throw new Exception(s"No such format $id"))
    }

    /** Obtain a previously registered SplitterFormat by class */
    def getFormat[P <: MinimizerPriorities](cls: Class[_ <: P]): SplitterFormat[P] = synchronized {
      formatsByCls.getOrElse(cls, throw new Exception(s"No format for class $cls")).asInstanceOf[SplitterFormat[P]]
    }
  }

  /** Defines a strategy for counting k-mers in Spark. */
  sealed trait CountMethod {
    /** Whether reverse complement data should be added at the input stage */
    def addRCToMainData(discount: Discount): Boolean = discount.normalize

    /** Resolve a definite count method (in case of Auto) */
    def resolve(priorities: MinimizerPriorities): CountMethod = this
  }

  /** Indicate that a strategy should be auto-selected */
  case object Auto extends CountMethod {
    override def resolve(priorities: MinimizerPriorities): CountMethod = {
      val r = if (priorities.numLargeBuckets > 0) Pregrouped else Simple
      println(s"Counting method: $r (use --method to override if running from command line)")
      r
    }
  }

  /** Pregrouped counting: groups and counts identical super-mers before counting k-mers.
   * Faster for datasets with high redundancy. */
  case object Pregrouped extends CountMethod {
    override def addRCToMainData(discount: Discount): Boolean = false

    override def toString = "Pregrouped"
  }

  /** Non-pregrouped: counts k-mers immediately. Faster for datasets with low redundancy. */
  case object Simple extends CountMethod {
    override def toString = "Simple"
  }

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
   * A file, or a directory containing multiple files with names like minimizers_{k}_{m}.txt,
   * in which case the best file will be selected. These files may specify an ordering.
   *
   * @param path the file, or directory to scan
   */
  final case class Path(path: String) extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] = {
      val s = new Sampling()
      val use = s.readMotifList(path, k, m).collect()
      println(s"${use.length}/${theoreticalMax(m)} $m-mers will become minimizers (loaded from $path)")
      use
    }
  }

  /**
   * Bundled minimizers on the classpath (only available for some values of k and m).
   */
  case object Bundled extends MinimizerSource {
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] = {
      BundledMinimizers.getMinimizers(k, m) match {
        case Some(internalMinimizers) =>
          println(s"${internalMinimizers.length}/${theoreticalMax(m)} $m-mers will become minimizers(loaded from classpath)")
          internalMinimizers.map(NTBitArray.encode(_).toInt)
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
    override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] =
      Array.range(0, 1 << (2 * m))
  }

  /** Programmatially generated minimizers. Will be used in the given order
   * if minimizerOrder = [[Given]] is used when configuring the [[Discount]] object. */
  final case class Generated(byPriority: Array[Int]) extends MinimizerSource {
    override def load(k: SeqID, m: SeqID)(implicit spark: SparkSession): Array[Int] =
      byPriority
  }

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
