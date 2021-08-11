/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

package discount

import org.rogach.scallop.Subcommand
import org.rogach.scallop.ScallopConf
import discount.hash.MotifSpace

object Commands {
  def run(conf: ScallopConf) {
    conf.subcommands.foreach {
      case command: RunnableCommand => command.run
      case _ =>
    }
  }
}

abstract class RunnableCommand(title: String) extends Subcommand(title) {
  def run(): Unit
}

/**
 * Configuration shared by the various tools implemented in this project.
 */
class CoreConf(args: Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](required = true, descr = "Length of k-mers")

  val normalize = opt[Boolean](descr = "Normalize k-mer orientation (forward/reverse complement) (default: off)",
    default = Some(false))

  val ordering = choice(Seq("frequency", "lexicographic", "given", "signature", "random"),
    default = Some("frequency"), descr = "Minimizer ordering (default frequency)")

  val minimizerWidth = opt[Int](required = true, name ="m", descr = "Width of minimizers (default 10)", default = Some(10))

  val sample = opt[Double](descr = "Fraction of reads to sample for motif frequency (default 0.01)",
    required = true, default = Some(0.01))

  val samplePartitions = opt[Int](name = "samplePartitions",
    descr = "Number of tasks to use for sampling (default 16)", required = false, default = Some(16))

  val minimizers = opt[String](descr = "File containing a set of minimizers to use (universal k-mer hitting set)")

  val rna = opt[Boolean](descr = "RNA mode (default is DNA)", default = Some(false), hidden = true)

  val long = opt[Boolean](default = Some(false), descr = "Read long sequence instead of short reads")

  def templateSpace = MotifSpace.ofLength(minimizerWidth(), rna())

  val maxSequenceLength = opt[Int](name = "maxlen", descr = "Maximum length of a single sequence/read (default 1000)",
    default = Some(1000))

  val multiline = opt[Boolean](descr = "Multiline sequences in FASTA files (default: off)", default = Some(false))

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
    } else Right(Unit)
  }
}
