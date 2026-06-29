/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.minimizer.{MinimizerPriorities, SpacedSeed}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.util.Random

/** Runnable commands for a command-line tool */
private[jnpersson] object Commands {
  def run(conf: ScallopConf): Unit = {
    conf.verify()
    val cmds = conf.subcommands.collect { case rc: RunCmd => rc }
    if (conf.args.contains("--detailed-help")) {
      conf.printHelp()
    } else if (cmds.isEmpty &&
      !conf.args.contains("--help") && !conf.args.contains("-h")) {
      conf.printHelp()
      throw new Exception("No command supplied. Nothing to do.")
    } else {
      for {c <- cmds} c.run()
    }
  }
}

private[jnpersson] abstract class RunCmd(title: String) extends Subcommand(title) {
  appendDefaultToDescription = true

  //to make sure --detailed-help is successfully parsed. Not used directly.
  val detailedHelp = opt[Boolean](descr = "Show additional help", noshort = true,
    hidden = !hasHiddenOptions) //if there are hidden options, then --detailed-help can be used to show them

  protected def hasHiddenOptions: Boolean = false

  def run(): Unit
}

/** Thrown when Scallop wants to exit the application, for example because the configuration could not be
 * parsed.
 * @param code exit code (as it would have been passed to System.exit)
 */
final case class ScallopExitException(code: Int) extends Exception

/**
 * Command-line configuration for minimizer schemes
 */
//noinspection TypeAnnotation
trait MinimizerCLIConf {
  this: ScallopConf =>

  protected def defaultK = 35
  val k = opt[Int](descr = "Length of each k-mer", default = Some(defaultK))

  protected def defaultMinimizerWidth = 10
  val minimizerWidth = opt[Int](name = "m", descr = "Width of minimizers",
    default = Some(defaultMinimizerWidth))

  validate (k) { k =>
    if (minimizerWidth() > k) {
      Left("-m must be <= -k")
    } else Right(())
  }

  protected def defaultOrdering: String = "lexicographic"

  protected def orderingChoices: Seq[String] = Seq("lexicographic", "random", "xor")

  protected def parseOrdering: String => MinimizerOrdering = _ match {
    case "lexicographic" => Lexicographic
    case "xor" | "random" => XORMask(defaultXORMask, canonicalMinimizers)
  }

  protected def orderingHidden: Boolean = true

  val ordering: ScallopOption[MinimizerOrdering] =
    choice(orderingChoices,
      default = Some(defaultOrdering), hidden = orderingHidden).
      map(parseOrdering)

  /** For the frequency ordering, whether to sample by sequence */
  protected def frequencyBySequence: Boolean = false

  /** For the XOR ordering, which mask to use */
  protected def defaultXORMask: Long = Random.nextLong()

  /** For some minimizer orderings, whether to use canonical orientation */
  protected def canonicalMinimizers = false

  def parseMinimizerSource: MinimizerSource =
    All

  def requireSuppliedK(): Unit = {
    if (!k.isSupplied) {
      throw new Exception("This command requires -k to be supplied")
    }
  }

  def defaultMinimizerSpaces: Int = 0

  val minimizerSpaces = opt[Int](name = "spaces",
    descr = "Number of masked out nucleotides in minimizer (spaced seed)",
    default = Some(defaultMinimizerSpaces))

  /** Apply a spaced seed mask to minimizer priorities */
  def seedMask(inner: MinimizerPriorities): MinimizerPriorities = {
    minimizerSpaces.toOption match {
      case None | Some(0) => inner
      case Some(s) => SpacedSeed(s, inner)
    }
  }

  //Replace the exit handler. We do not want to call System.exit as we may be running inside a spark cluster
  //that needs to terminate gracefully. Throw an exception that can be caught in the main function
  //and handled there.
  exitHandler = (exitCode: Int) => {
    throw ScallopExitException(exitCode)
  }
}
