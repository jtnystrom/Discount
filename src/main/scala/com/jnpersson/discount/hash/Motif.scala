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

package com.jnpersson.discount.hash

import com.jnpersson.discount.NTSeq

import scala.language.postfixOps

/**
 * The attributes of a motif (minimizer), except its position.
 * @param pattern The motif/minimizer
 * @param rank priority/unique ID of this motif. Lower value indicates higher priority.
 * @param valid Whether this is a valid minimizer/motif
 */
final case class Features(pattern: NTSeq, rank: Int, valid: Boolean)

object Motif {
  val Empty = Motif(0, Features("", 0, false))

  val INVALID = -1
}

/**
 * A motif from a nucleotide sequence.
 * @param pos The position of the motif in the sequence
 * @param features The features of the motif
 */
final case class Motif(pos: Int, features: Features) {
  /** The nucleotide sequence */
  def pattern = features.pattern

  override def toString = "[%s,%02d]".format(pattern, pos)

  def motif = this
}
