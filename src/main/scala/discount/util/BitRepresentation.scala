
/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */
package discount.util
import discount.NTSeq

import scala.annotation.switch
import scala.collection.immutable._

class InvalidNucleotideException(val invalidChar: Char) extends Exception

/**
 * Helper functions for working with a low level bit representation of nucleotide sequences.
 */
object BitRepresentation {

  /*
   * The encoded representation is a mostly arbitrary choice. The values chosen here
   * have the advantage that the DNA complement can easily be obtained by XORing with all 1:s.
   */
  val A: Byte = 0x0
  val C: Byte = 0x1
  val G: Byte = 0x2
  val T: Byte = 0x3
  val U: Byte = 0x3 //In RNA, instead of T. Note: RNA support is currently only partial.

  val twobits = List(A, C, T, G)

  /**
   * Complement of a single BP.
   */
  def complementOne(byte: Byte): Int = complement(byte) & 0x3

  /**
   * Complement of a number of BPs packed in a byte.
   */
  def complement(byte: Byte): Byte = {
    (byte ^ 0xff).toByte
  }

  /**
   * Map a quad-string (four letters) to an encoded byte
   */
  def quadToByte(quad: NTSeq): Byte = quadToByte(quad, 0)

  /**
   * Map a single byte to a quad-string for unpacking.
   * Precomputed lookup array.
   */
  val byteToQuadLookup: Array[NTSeq] = {
    val r = new Array[NTSeq](256)
    for (i <- 0 to 255) {
      val b = i.toByte
      val str = byteToQuadCompute(b)
      r(b - Byte.MinValue) = str
    }
    r
  }

  /**
   * Convert a single byte to the "ACTG" format (a 4 letter string)
   */
  private def byteToQuadCompute(byte: Byte): NTSeq = {
    var res = ""
    val chars = for (i <- 0 to 3) {
      val ptn = ((byte >> ((3 - i) * 2)) & 0x3)
      val char = twobitToChar(ptn.toByte)
      res += char
    }
    res
  }

  /**
   * Unpack a byte to a 4-character string (quad).
   */
  def byteToQuad(byte: Byte): NTSeq = byteToQuadLookup(byte - Byte.MinValue)

  /**
   * Convert a single nucleotide from string (char) representation to "twobit" representation.
   */
  def charToTwobit(char: Char): Byte = (char: @switch) match {
      case 'A' => A
      case 'C' => C
      case 'G' => G
      case 'T' => T
      case 'U' => U
      case _ => throw new InvalidNucleotideException(char)
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
   * Convert an NT quad (string of length 4) to encoded
   * byte form. The string will be padded on the right with
   * 'A' if it's too short.
   */
  def quadToByte(quad: String, offset: Int): Byte = {
    var res = 0
    var i = offset
    val end = offset + 4
    while (i < end) {
      val c = if (i >= quad.length) 'A' else quad.charAt(i)
      val twobit = charToTwobit(c)
      if (i == 0) {
        res = twobit
      } else {
        res = (res << 2) | twobit
      }
      i += 1
    }
    res.toByte
  }

  /*
	 * Convert a string to an array of quads.
	 */
  def stringToBytes(bps: NTSeq): Array[Byte] = {
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
  def bytesToString(bytes: Array[Byte], builder: StringBuilder, offset: Int, size: Int): NTSeq = {
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
