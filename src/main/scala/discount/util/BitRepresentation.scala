/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */
package discount.util

import scala.annotation.switch
import scala.collection.immutable._

class InvalidNucleotideException(val invalidChar: Char) extends Exception

/**
 * Helper functions for working with a low level bit representation of nucleotide sequences.
 * (Companion object)
 */
object BitRepresentation {
  val A: Byte = 0x0
  val C: Byte = 0x1
  val G: Byte = 0x2
  val T: Byte = 0x3
  val U: Byte = 0x3 //In RNA, instead of T. Note: RNA support is currently only partial.

  val twobits = List(A, C, T, G)

  val byteToQuad = new Array[String](256)

  def quadToByte(quad: String): Byte = quadToByte(quad, 0)

  //precompute conversion table once
  for (i <- 0 to 255) {
    val b = i.toByte
    val str = byteToQuadCompute(b)
    byteToQuad(b - Byte.MinValue) = str
  }

  /**
   * Convert a single nucleotide from string representation to "twobit" representation.
   */
  val charToTwobit: Array[Byte] = (0 to 'U').map(x => cmpCharToTwobit(x.toChar)).toArray

  private def cmpCharToTwobit(char: Char): Byte = char match {
      case 'A' => A
      case 'C' => C
      case 'G' => G
      case 'T' => T
      case 'U' => U
      case _ => -1
    }

  /**
   * Convert a single BP from twobit representation to string representation.
   */
  def twobitToChar(byte: Byte): Char = {
    (byte: @switch) match {
      case 0 => 'A'
      case 1 => 'C'
      case 2 => 'G'
      case 3 => 'T'
    }
  }

  /**
   * Convert a single byte to the "ACTG" format (a 4 letter string)
   */
  def byteToQuadCompute(byte: Byte): String = {
    var res = ""
    val chars = for (i <- 0 to 3) {
      val ptn = ((byte >> ((3 - i) * 2)) & 0x3)
      val char = twobitToChar(ptn.toByte)
      res += char
    }
    res
  }

  /**
   * Lookup array for conversion of strings to compact form.
   * Exploits the fact that bits 6 and 7 (0x6) are distinct for
   * all four NT characters.
   */
  private val quadLookup: Array[Byte] = {
    val r = new Array[Byte](256)
    val chars = List('A', 'C', 'T', 'G')
    for {
      c1 <- chars
      c2 <- chars
      c3 <- chars
      c4 <- chars
      quad = ((c1 & 6) << 5) | ((c2 & 6) << 3) |
        ((c3 & 6) << 1) | ((c4 & 6) >> 1)
      encoded = (charToTwobit(c1) << 6) | (charToTwobit(c2) << 4) |
        (charToTwobit(c3) << 2) | charToTwobit(c4)
    } {
      r(quad) = encoded.toByte
    }
    r
  }

  /**
   * Convert an NT quad (string of length 4) to encoded
   * byte form. The string will be padded on the right with
   * 'A' if it's too short.
   */
  def quadToByte(quad: String, offset: Int): Byte = {
    var c1 = 'A'
    var c2 = 'A'
    var c3 = 'A'
    var c4 = 'A'

    val len = quad.length
    c1 = quad.charAt(offset)
    if (offset + 1 < len) {
      c2 = quad.charAt(offset + 1)
      if (offset + 2 < len) {
        c3 = quad.charAt(offset + 2)
        if (offset + 3 < len) {
          c4 = quad.charAt(offset + 3)
        }
      }
    }
    val encQuad = ((c1 & 6) << 5) | ((c2 & 6) << 3) |
      ((c3 & 6) << 1) | ((c4 & 6) >> 1)
    quadLookup(encQuad)
  }

  /**
   * Convert a byte to a 4-character string.
   */
  def byteToQuad(byte: Byte): String = byteToQuad(byte - Byte.MinValue)

  /**
   * Complement of a single BP.
   */
  def complementOne(byte: Byte) = complement(byte) & 0x3

  /**
   * Complement of a number of BPs packed in a byte.
   */
  def complement(byte: Byte) = {
    //	  println("Complement " + byte)
    (byte ^ 0xff).toByte
  }

  /*
	 * Convert a string to an array of quads.
	 */
  def stringToBytes(bps: String): Array[Byte] = {
    var i = 0
    val rsize = (bps.size - 1) / 4
    val r = new Array[Byte](rsize + 1)
    while (i <= rsize) {
      r(i) = quadToByte(bps, i * 4)
      i += 1
    }
    r
  }

  /**
   * Convert a byte array of quads to a string. The length of the
   * resulting string must be supplied.
   */
  def bytesToString(bytes: Array[Byte], builder: StringBuilder, offset: Int, size: Int): String = {
    val startByte = offset / 4

    var i = startByte
    while (i < bytes.size) {
      if (builder.size < size) {
        if (i == startByte) {
          builder.append(byteToQuad(bytes(i)).substring(offset % 4, 4))
        } else {
          builder.append(byteToQuad(bytes(i)))
        }
      }
      i += 1
    }
    builder.substring(0, size)
  }
}
