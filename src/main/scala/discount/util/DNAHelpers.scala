/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package discount.util

import scala.annotation.{switch}

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

}
