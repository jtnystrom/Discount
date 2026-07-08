/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.util.NTBitArray
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf


/** Extra configuration options relating to advanced minimizer orderings */
//noinspection TypeAnnotation
trait AdvancedMinimizerConfiguration extends MinimizerCLIConf {
  this: ScallopConf =>

  val normalize = opt[Boolean](descr = "Normalize k-mer orientation (forward/reverse complement)")

  override protected def parseOrdering(x: String): MinimizerOrdering = {
    //XORMask has its own logic for canonicals.
    //normalize() requires canonicalMinimizers so we force it in this case.
    if (x != "xor" && x != "random" && (canonicalMinimizers() || normalize()))
      Canonical(parseOrderingNonCanonical(x))
    else
      parseOrderingNonCanonical(x)
  }

  validate (k) { k =>
    if (normalize() && (k % 2 == 0)) {
      Left(s"--normalize is only available for odd values of k, but $k was given")
    } else Right(())
  }

  val extendMinimizers = opt[Int](descr = "Extended width of minimizers")

  protected def extendedWithSuffix: Boolean = false

  validate (extendMinimizers) { e =>
    if (minimizerWidth() >= e) {
      Left ("--extendMinimizers must be > m")
    } else if (k() < e) {
      Left("--extendMinimizers must be <= k")
    } else Right(())
  }

  protected def extendMinimizersIfConfigured(inner: MinimizerSource): MinimizerSource =
    extendMinimizers.toOption match {
      case Some(e) => Extended(inner, e, canonicalMinimizers(), extendedWithSuffix)
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

  val sample = opt[Double](descr = "Fraction of reads to sample for minimizer frequency",
    required = true, default = Some(0.01))

  validate (sample) { s =>
    if (s <= 0 || s > 1) {
      Left(s"--sample must be > 0 and <= 1 ($s was given)")
    } else Right(())
  }

  override protected def orderingChoices: Seq[String] =
    Seq("frequency", "lexicographic", "given", "random", "xor")

  override protected def orderingHidden: Boolean = false

  override protected def parseOrderingNonCanonical(x: String): MinimizerOrdering = x match {
    case "frequency" => Frequency
    case "lexicographic" => Lexicographic
    case "given" => Given
    case "xor" | "random" => XORMask(defaultXORMask, canonicalMinimizers())
  }
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
