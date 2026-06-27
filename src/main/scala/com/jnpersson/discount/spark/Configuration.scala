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

package com.jnpersson.discount.spark

import com.jnpersson.discount._
import com.jnpersson.discount.hash.{Extended, MinimizerPriorities, SpacedSeed}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.util.Random

/** Runnable commands for a command-line tool */
private[jnpersson] object Commands {
  def run(conf: ScallopConf)(spark: SparkSession) : Unit = {
    conf.verify()
    val cmds = conf.subcommands.collect { case rc: RunCmd => rc }
    if (cmds.isEmpty) {
      throw new Exception("No command supplied (please see --help). Nothing to do.")
    }
    for { c <- cmds } c.run(spark)
  }
}

private[jnpersson] abstract class RunCmd(title: String) extends Subcommand(title) {
  def run(implicit spark: SparkSession): Unit
}

/**
 * Main command-line configuration
 * @param args command-line arguments
 */
class Configuration(args: Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](descr = "Length of each k-mer")

  val normalize = opt[Boolean](descr = "Normalize k-mer orientation (forward/reverse complement)")

  val minimizerWidth = opt[Int](name = "m", descr = "Width of minimizers (default 10)",
    default = Some(10))

  validate (k) { k =>
    if (minimizerWidth() > k) {
      Left("-m must be <= -k")
    } else if (normalize() && (k % 2 == 0)) {
      Left(s"--normalize is only available for odd values of k, but $k was given")
    } else Right(Unit)
  }

  def defaultOrdering: String = "frequency"

  /** For the frequency ordering, whether to sample by sequence */
  protected def frequencyBySequence: Boolean = false

  /** For the XOR ordering, which mask to use */
  protected def defaultXORMask: Long = Random.nextLong()

  /** For some minimizer orderings, whether to use canonical orientation */
  protected def canonicalMinimizers = false

  val ordering: ScallopOption[MinimizerOrdering] =
    choice(Seq("frequency", "lexicographic", "given", "signature", "random", "xor"),
    default = Some(defaultOrdering), descr = s"Minimizer ordering (default $defaultOrdering).").
    map {
      case "frequency" => Frequency(frequencyBySequence)
      case "lexicographic" => Lexicographic
      case "given" => Given
      case "signature" => Signature
      case "xor" | "random" => XORMask(defaultXORMask, canonicalMinimizers)
    }

  val sample = opt[Double](descr = "Fraction of reads to sample for minimizer frequency (default 0.01)",
    required = true, default = Some(0.01))

  validate (sample) { s =>
    if (s <= 0 || s > 1) {
      Left(s"--sample must be > 0 and <= 1 ($s was given)")
    } else Right(Unit)
  }

  def defaultAllMinimizers = false
  val allMinimizers = toggle(name="allMinimizers", descrYes = "Use all m-mers as minimizers",
    descrNo = "Use a provided or internal precomputed minimizer set", default = Some(defaultAllMinimizers))

  val minimizers = opt[String](
    descr = "File containing a set of minimizers to use (universal k-mer hitting set), or a directory of such universal hitting sets")

  protected def defaultMaxSequenceLength = 10000000 //10M bps
  val maxSequenceLength = opt[Int](name = "maxlen",
    descr = s"Maximum length of a single sequence/read (default $defaultMaxSequenceLength)",
    default = Some(defaultMaxSequenceLength))

  val method: ScallopOption[CountMethod] =
    choice(Seq("simple", "pregrouped", "auto"),
    default = Some("auto"), descr = "Counting method (default auto).").
    map {
      case "auto" => Auto
      case "simple" => Simple
      case "pregrouped" => Pregrouped
    }

  val partitions = opt[Int](descr = "Number of shuffle partitions/parquet buckets for indexes (default 200)", default = Some(200))

  val extendMinimizers = opt[Int](descr = "Extended width of minimizers")

  protected def extendedWithSuffix: Boolean = false

  validate (extendMinimizers) { e =>
    if (minimizerWidth() >= e) {
      Left ("--extendMinimizers must be > m")
    } else if (k() < e) {
      Left("--extendMinimizers must be <= k")
    } else Right(Unit)
  }

  def parseMinimizerSource: MinimizerSource = {
    val inner = minimizers.toOption match {
      case Some(path) => Path(path)
      case _ => if (allMinimizers()) {
        All
      } else {
        Bundled
      }
    }
    extendMinimizersIfConfigured(inner)
  }

  protected def extendMinimizersIfConfigured(inner: MinimizerSource): MinimizerSource =
    extendMinimizers.toOption match {
      case Some(e) => Extended(inner, e, canonicalMinimizers, extendedWithSuffix)
      case _ => inner
    }

  def requireSuppliedK(): Unit = {
    if (!k.isSupplied) {
      throw new Exception("This command requires -k to be supplied")
    }
  }

  def defaultMinimizerSpaces: Int = 0

  val minimizerSpaces = opt[Int](name = "spaces",
    descr = s"Number of masked out nucleotides in minimizer (spaced seed, default $defaultMinimizerSpaces)",
    default = Some(defaultMinimizerSpaces))

  def seedMask(inner: MinimizerPriorities): MinimizerPriorities = {
    minimizerSpaces.toOption match {
      case None | Some(0) => inner
      case Some(s) => SpacedSeed(s, inner)
    }
  }

  def discount(implicit spark: SparkSession): Discount = {
    requireSuppliedK()
    new Discount(k(), parseMinimizerSource, minimizerWidth(), ordering(), sample(), maxSequenceLength(), normalize(),
      method(), partitions())
  }

  def discount(p: IndexParams)(implicit spark: SparkSession): Discount = {
    val session = SparkTool.newSession(spark, p.buckets)
    new Discount(p.k, parseMinimizerSource, p.m, ordering(), sample(), maxSequenceLength(), normalize(), method(),
      p.buckets)(session)
  }
}
