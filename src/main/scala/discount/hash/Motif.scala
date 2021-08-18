/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

package discount.hash

import discount.NTSeq
import discount.hash.PosRankWindow._
import discount.hash.Motif._

import scala.language.postfixOps

object Motif {
  val Empty = Motif(0, Features("", 0, false))

  /**
   * The attributes of a motif, except its position.
   * @param rank priority/unique ID of this motif. Lower value indicates higher priority.
   */
  final case class Features(pattern: NTSeq, rank: Int, valid: Boolean)
}

/** A single motif (a potential minimizer) in a nucleotide sequence.
 * @param pos
 * @param features
 */
final case class Motif(pos: Int, features: Features) extends MotifContainer {
  /** The nucleotide sequence */
  def pattern: NTSeq = features.pattern

  override def toString = "[%s,%02d]".format(pattern, pos)

  def motif = this
}
