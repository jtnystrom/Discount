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

package discount.util

import scala.util.Random
import scala.annotation.{switch, tailrec}

object DNAHelpers {

  /**
   * Obtain the complement of a single nucleotide.
   */
  def charComplement(bp: Char): Char =
    (bp: @switch) match {
    case 'A' => 'T'
    case 'C' => 'G'
    case 'T' => 'A'
    case 'G' => 'C'
    case 'N' => 'N'
    case _   => throw new Exception("Error: " + bp + " is not a nucleotide")
  }

  /**
   * Obtain the complement of a string of nucleotides.
   */
  def complement(data: String): String = {
    var i = 0
    val cs = new Array[Char](data.size)
    while (i < data.size) {
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

  /**
   * Extend a given string by a number of random basepairs
   */
  @tailrec
  def extendSeq(seq: String,
                steps: Int,
                generator: Random = new Random(),
                basemap: Int => Char = Map(0 -> 'A',
                  1 -> 'C',
                  2 -> 'G',
                  3 -> 'T')): String = {
    if (steps == 0) {
      seq
    } else {
      extendSeq(seq + basemap(generator.nextInt(4)), steps - 1, generator, basemap)
    }
  }

  /**
   * Return a random sequence of basepairs as a string
   */
  def randomSequence(length: Int): String = extendSeq("", length)

  def kmerPrefix(seq: String, k: Int) = seq.substring(0, k - 1)
  def kmerPrefix(seq: StringBuilder, k: Int) = seq.substring(0, k - 1)

  def kmerSuffix(seq: String, k: Int) = seq.substring(seq.length() - (k - 1))
  def kmerSuffix(seq: StringBuilder, k: Int) = seq.substring(seq.size - (k - 1))

  def withoutPrefix(seq: String, k: Int) = seq.substring(k - 1)

  def withoutSuffix(seq: String, k: Int) = seq.substring(0, seq.length() - (k - 1))

}
