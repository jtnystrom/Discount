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

package discount.hash

import discount.NTSeq
import discount.util.BPBuffer

import scala.collection.Seq
import scala.collection.mutable

object MotifSpace {
  val all1mersDNA = Seq("A", "C", "G", "T")
  val all1mersRNA = Seq("A", "C", "G", "U")

  /**
   * Generate all sequences of the given length in lexicographic order.
   * @param length
   * @param rna
   * @return
   */
  def motifsOfLength(length: Int, rna: Boolean = false): Seq[String] = {
    val bases = if (rna) all1mersRNA else all1mersDNA
    if (length == 1) {
      bases
    } else if (length > 1) {
      motifsOfLength(length - 1, rna).flatMap(x => bases.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def ofLength(w: Int, rna: Boolean): MotifSpace = using(motifsOfLength(w, rna))

  def using(mers: Seq[String]) = new MotifSpace(mers.toArray)

  def fromTemplateWithValidSet(template: MotifSpace, validMers: Iterable[String]): MotifSpace = {
    val validSet = validMers.to[mutable.Set]
    template.copy(byPriority = template.byPriority.filter(validSet.contains))
  }
}

/**
 * A set of motifs that can be used, and their relative priorities.
 * @param byPriority Motifs in the space ordered from high priority to low
 */
final case class MotifSpace(byPriority: Array[NTSeq]) {
  val width = byPriority.map(_.length()).max
  def maxMotifLength = width
  val minMotifLength = byPriority.map(_.length()).min

  @volatile
  private var lookup = Map.empty[NTSeq, Features]

  def getFeatures(pattern: NTSeq): Features = {
    if (!lookup.contains(pattern)) {
      synchronized {
        if (!lookup.contains(pattern)) {
          val f = new Features(pattern, priorityOf(pattern), true)
          lookup += pattern -> f
        }
      }
    }
    lookup(pattern)
  }

  def get(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, getFeatures(pattern))
  }

  def create(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, new Features(pattern, priorityOf(pattern), true))
  }

  //4 ^ width
  val maxMotifs = 4 << (width * 2 - 2)

  //bit shift distance
  val shift = 32 - (width * 2)

  /**
   * Compute lookup index for a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * @param m
   * @return
   */
  def motifToInt(m: NTSeq) = {
    val wrapped = BPBuffer.encode(m)
    BPBuffer.computeIntArrayElement(wrapped.data, 0, width, 0) >>> shift
  }

  /**
   * Maps the bit-encoded integer form of each motif to its priority/rank
   * priorityLookup always has size 4^width. Invalid entries will have priority -1.
   */
  val priorityLookup: Array[Int] = Array.fill(maxMotifs)(-1)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    priorityLookup(motifToInt(motif)) = pri
  }

  def priorityOf(mk: NTSeq) =
    priorityLookup(motifToInt(mk))

  val compactBytesPerMotif = if (width > 8) 4 else if (width > 4) 2 else 1
}