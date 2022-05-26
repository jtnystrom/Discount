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

package com.jnpersson.discount.util

import scala.annotation.{switch, tailrec}

object DNAHelpers {

  /**
   * Obtain the complement of a single nucleotide.
   */
  def charComplement(bp: Char): Char =
    (bp: @switch) match {
    case 'A' | 'a' => 'T'
    case 'C' | 'c' => 'G'
    case 'T' | 't' => 'A'
    case 'G' | 'g' => 'C'
    case 'N' | 'n' => 'N'
    case _   => throw new InvalidNucleotideException(bp)
  }

  /**
   * Obtain the complement of a string of nucleotides.
   */
  def complement(data: String): String = {
    var i = 0
    val cs = new Array[Char](data.length)
    while (i < data.length) {
      cs(i) = charComplement(data.charAt(i))
      i += 1
    }
    new String(cs)
  }

  /**
   * Obtain the reverse complement of a string of nucleotides.
   */
  def reverseComplement(data: String): String = {
    val b = new StringBuilder()
    b.sizeHint(data.length)
    var i = data.length
    while (0 < i) {
      i -= 1
      b += charComplement(data.charAt(i))
    }
    b.toString()
  }
}
