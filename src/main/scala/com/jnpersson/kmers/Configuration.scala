/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.minimizer.{Extended, MinimizerPriorities, SpacedSeed}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.util.Random

/** Runnable commands for a command-line tool */
private[jnpersson] object Commands {
  def run(conf: ScallopConf): Unit = {
    conf.verify()
    val cmds = conf.subcommands.collect { case rc: RunCmd => rc }
    if (cmds.isEmpty) {
      throw new Exception("No command supplied (please see --help). Nothing to do.")
    }
    for { c <- cmds } c.run()
  }
}

private[jnpersson] abstract class RunCmd(title: String) extends Subcommand(title) {
  def run(): Unit
}

/**
 * Main command-line configuration
 *
 * @param args command-line arguments
 */
//noinspection TypeAnnotation
class Configuration(args: Seq[String]) extends ScallopConf(args) {
  protected def defaultK = 35
  val k = opt[Int](descr = s"Length of each k-mer (default $defaultK)", default = Some(defaultK))

  protected def defaultMinimizerWidth = 10
  val minimizerWidth = opt[Int](name = "m", descr = s"Width of minimizers (default $defaultMinimizerWidth)",
    default = Some(defaultMinimizerWidth))

  validate (k) { k =>
    if (minimizerWidth() > k) {
      Left("-m must be <= -k")
    } else Right(Unit)
  }

  def defaultOrdering: String = "lexicographic"

  val ordering: ScallopOption[MinimizerOrdering] =
    choice(Seq("lexicographic", "random", "xor"),
      default = Some(defaultOrdering), hidden = true).
      map {
        case "lexicographic" => Lexicographic
        case "xor" | "random" => XORMask(defaultXORMask, canonicalMinimizers)
      }

  /** For the frequency ordering, whether to sample by sequence */
  protected def frequencyBySequence: Boolean = false

  /** For the XOR ordering, which mask to use */
  protected def defaultXORMask: Long = Random.nextLong()

  /** For some minimizer orderings, whether to use canonical orientation */
  protected def canonicalMinimizers = false

  protected def defaultMaxSequenceLength = 10000000 //10M bps
  val maxSequenceLength = opt[Int](name = "maxlen",
    descr = s"Maximum length of a single sequence/read (default $defaultMaxSequenceLength)",
    default = Some(defaultMaxSequenceLength))


  def parseMinimizerSource: MinimizerSource =
    All

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

  val sample = opt[Double](descr = "Fraction of reads to sample for minimizer frequency (default 0.01)",
    required = true, default = Some(0.01), hidden = true)
}

/** Extra configuration options relating to advanced minimizer orderings */
//noinspection TypeAnnotation
trait AdvancedMinimizerOrderingsConfiguration {
  this: Configuration =>

  val normalize = opt[Boolean](descr = "Normalize k-mer orientation (forward/reverse complement)")

  validate (k) { k =>
    if (normalize() && (k % 2 == 0)) {
      Left(s"--normalize is only available for odd values of k, but $k was given")
    } else Right(Unit)
  }

  val extendMinimizers = opt[Int](descr = "Extended width of minimizers")

  protected def extendedWithSuffix: Boolean = false

  validate (extendMinimizers) { e =>
    if (minimizerWidth() >= e) {
      Left ("--extendMinimizers must be > m")
    } else if (k() < e) {
      Left("--extendMinimizers must be <= k")
    } else Right(Unit)
  }

  protected def extendMinimizersIfConfigured(inner: MinimizerSource): MinimizerSource =
    extendMinimizers.toOption match {
      case Some(e) => Extended(inner, e, canonicalMinimizers, extendedWithSuffix)
      case _ => inner
    }

  override def parseMinimizerSource: MinimizerSource = {
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

  def defaultAllMinimizers = false

  val allMinimizers = toggle(name="allMinimizers", descrYes = "Use all m-mers as minimizers",
    descrNo = "Use a provided or internal precomputed minimizer set", default = Some(defaultAllMinimizers))

  val minimizers = opt[String](
    descr = "File containing a set of minimizers to use (universal k-mer hitting set), or a directory of such universal hitting sets")

  override val sample = opt[Double](descr = "Fraction of reads to sample for minimizer frequency (default 0.01)",
    required = true, default = Some(0.01))

  validate (sample) { s =>
    if (s <= 0 || s > 1) {
      Left(s"--sample must be > 0 and <= 1 ($s was given)")
    } else Right(Unit)
  }

  override val ordering: ScallopOption[MinimizerOrdering] =
    choice(Seq("frequency", "lexicographic", "given", "signature", "random", "xor"),
      default = Some(defaultOrdering), descr = s"Minimizer ordering (default $defaultOrdering).").
      map {
        case "frequency" => Frequency(frequencyBySequence)
        case "lexicographic" => Lexicographic
        case "given" => Given
        case "signature" => Signature
        case "xor" | "random" => XORMask(defaultXORMask, canonicalMinimizers)
      }
}
