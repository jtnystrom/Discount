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

import org.rogach.scallop.Subcommand
import org.rogach.scallop.ScallopConf
import com.jnpersson.discount.hash.{MotifSpace}
import scala.collection


/** Runnable commands for a command-line tool */
object Commands {
  def run(conf: ScallopConf): Unit = {
    conf.verify()
    val cmds = conf.subcommands.collect { case rc: RunnableCommand => rc }
    if (cmds.isEmpty) {
      throw new Exception("No command supplied (please see --help). Nothing to do.")
    }
    for { c <- cmds } { c.run() }
  }
}

abstract class RunnableCommand(title: String) extends Subcommand(title) {
  def run(): Unit
}

/**
 * Main command-line configuration
 * @param args
 */
class Configuration(args: collection.Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](required = true, descr = "Length of each k-mer")

  val normalize = opt[Boolean](descr = "Normalize k-mer orientation (forward/reverse complement) (default: off)",
    default = Some(false))

  val ordering = choice(Seq("frequency", "lexicographic", "given", "signature", "random"),
    default = Some("frequency"), descr = "Minimizer ordering (default frequency)")

  val minimizerWidth = opt[Int](required = true, name ="m", descr = "Width of minimizers (default 10)",
    default = Some(10))

  val sample = opt[Double](descr = "Fraction of reads to sample for motif frequency (default 0.01)",
    default = Some(0.01))

  val minimizers = opt[String](
    descr = "File containing a set of minimizers to use (universal k-mer hitting set), or a directory of such universal hitting sets")

  val maxSequenceLength = opt[Int](name = "maxlen",
    descr = "Maximum length of a single sequence/read (default 1000000)", default = Some(1000000))

  validate (minimizerWidth, k, normalize) { (m, k, n) =>
    if (m >= k) {
      Left("-m must be < -k")
    } else if (m > 15) {
      //In testing, larger values than 15 have so far been impractical as they lead to
      //a big memory requirement for the minimizer lookup array.
      //They also cannot be encoded as integers.
      Left("-m must be <= 15")
    } else if (n && (k%2 == 0)) {
      Left(s"--normalize is only available for odd values of k, but $k was given")
    } else Right(())
  }
}
