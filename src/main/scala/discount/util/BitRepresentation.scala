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
    //    quadToByte += ((str, b))
  }

  /**
   * Convert a single BP from string representation to "twobit" representation.
   */
  def charToTwobit(char: Char): Byte = {
    (char: @switch) match {
      case 'A' => A
      case 'C' => C
      case 'G' => G
      case 'T' => T
      case 'U' => U
      case _ => throw new InvalidNucleotideException(char)
    }
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

  //If the string is too short, it will be padded on the right with 'A' (0).
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

  /**
   * Convert a byte to a 4-character string.
   */
  def byteToQuad(byte: Byte): String = byteToQuad(byte - Byte.MinValue)

  import scala.language.implicitConversions

  implicit def toByte(int: Int) = int.toByte

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
