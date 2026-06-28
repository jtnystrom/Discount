/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

package com.jnpersson.kmers.util

import com.jnpersson.kmers._
import scala.annotation.switch
import scala.collection.immutable._

/** Thrown when we encounter a letter in the sequence input that we do not expect to see */
class InvalidNucleotideException(val invalidChar: Char) extends Exception

/**
 * Helper functions for working with a low level bit representation of nucleotide sequences.
 */
object BitRepresentation {

  /*
   * The encoded representation is a mostly arbitrary choice. The values chosen here
   * have the advantage that the DNA complement can easily be obtained by XORing with all 1:s.
   */
  final val A = 0
  final val C = 1
  final val G = 2
  final val T = 3
  final val U = T //In RNA, instead of T. Note: RNA support is currently only partial.

  val twobits: List[Byte] = List(A, C, T, G).map(_.toByte)

  final val WHITESPACE = 4
  final val INVALID = 5

  /**
   * Complement of a single BP.
   */
  def complementOne(byte: Byte): Int = complement(byte) & 0x3

  /**
   * Complement of a number of BPs packed in a byte.
   */
  def complement(byte: Byte): Byte =
    (byte ^ 0xff).toByte

  //Adapted from kraken2 mmscanner.cc
  //Original credit: adapted for 64-bit DNA use from public domain code at:
  //https://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel
  private def swapNTSequence(encodedNTs: Long): Long = {
    var kmer = encodedNTs
    // Reverse bits (leaving bit pairs - nucleotides - intact)
    // swap consecutive pairs
    kmer = ((kmer & 0xCCCCCCCCCCCCCCCCL) >>> 2) | ((kmer & 0x3333333333333333L) << 2)
    // swap consecutive nibbles
    kmer = ((kmer & 0xF0F0F0F0F0F0F0F0L) >>> 4) | ((kmer & 0x0F0F0F0F0F0F0F0FL) << 4)
    // swap consecutive bytes
    kmer = ((kmer & 0xFF00FF00FF00FF00L) >>> 8) | ((kmer & 0x00FF00FF00FF00FFL) << 8)
    // swap consecutive byte pairs
    kmer = ((kmer & 0xFFFF0000FFFF0000L) >>> 16) | ((kmer & 0x0000FFFF0000FFFFL) << 16)
    // swap halves of 64-bit word
    (kmer >>> 32) | (kmer << 32)
  }

  /** Reverse complement an encoded NT sequence that is right-aligned in a long */
  def reverseComplement(encodedNTs: Long, width: Int, complementMask: Long): Long = {
    var kmer = swapNTSequence(encodedNTs)
    kmer = kmer >>> (64 - width * 2)
    kmer ^ complementMask
  }

  /** Reverse complement an encoded NT sequence that is left-aligned in a long */
  def reverseComplementLeftAligned(encodedNTs: Long, complementMask: Long): Long =
    swapNTSequence(encodedNTs) ^ complementMask

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
    for (i <- 0 to 3) {
      val ptn = (byte >> ((3 - i) * 2)) & 0x3
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
   * Returns one of the twobit codes, or WHITESPACE for skippable whitespace.
   */
  def charToTwobit(char: Char): Byte = (char: @switch) match {
      case 'A' | 'a' => A.toByte
      case 'C' | 'c' => C.toByte
      case 'G' | 'g' => G.toByte
      case 'T' | 't' => T.toByte
      case 'U' | 'u' => U.toByte
      case '\n' | '\r' => WHITESPACE.toByte
      case _ => throw new InvalidNucleotideException(char)
    }

  /**
   * Convert a single nucleotide from string (char) representation to "twobit" representation.
   * Does not throw an exception, but returns INVALID on invalid characters.
   */
  def charToTwobitWithInvalid(char: Char): Byte = (char: @switch) match {
    case 'A' | 'a' => A.toByte
    case 'C' | 'c' => C.toByte
    case 'G' | 'g' => G.toByte
    case 'T' | 't' => T.toByte
    case 'U' | 'u' => U.toByte
    case '\n' | '\r' => WHITESPACE.toByte
    case _ => INVALID.toByte
  }

  /**
   * Convert a single BP from twobit representation to string representation.
   */
  def twobitToChar(byte: Byte): Char = (byte: @switch) match {
      case A => 'A'
      case C => 'C'
      case G => 'G'
      case T => 'T'
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
    while (i < bytes.length) {
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
