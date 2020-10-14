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
    for (com <- conf.subcommands) {
      com match {
      case command: RunnableCommand =>
        command.run
      case _ =>
      }
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
  val k = opt[Int](required = true, descr = "Length of each k-mer")

  val addRC = opt[Boolean](name = "addRC", descr = "Add reverse complements")

  val ordering = opt[String](descr = "Minimizer ordering (frequency/lexicographic/signature)",
    default = Some("frequency"))

  val minimizerWidth = opt[Int](required = true, name ="m", descr = "Width of minimizers", default = Some(10))

  val sample = opt[Double](descr = "Fraction of reads to sample for motif frequency",
    required = true, default = Some(0.01))

  val numCPUs = opt[Int](name = "numCPUs",
    descr = "Total number of CPUs expected to be available to executors (for sampling)",
    required = false, default = Some(16))

  val motifSet = opt[String](descr = "Set of motifs to use (universal set)")

  val rna = opt[Boolean](descr = "RNA mode (default is DNA)", default = Some(false))

  val long = toggle(default = Some(false), descrYes = "Read long sequence instead of short reads")

  def preferredSpace = MotifSpace.ofLength(minimizerWidth(), rna())

  val maxSequenceLength = opt[Int](name = "maxlen", descr = "Maximum length of a single sequence/read",
    default = Some(1000))
}
