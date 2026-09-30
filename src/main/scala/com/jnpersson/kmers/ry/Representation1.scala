package com.jnpersson.kmers.ry

import com.jnpersson.kmers.NTSeq
import com.jnpersson.kmers.util.{InvalidNucleotideException, NTBitArray}

import scala.annotation.switch

/** Functions to support the 1 bit per character encoding of the RY
 * alphabet.
 */
object Representation1 {

  /** Convert char to bit. This mapping is compatible with the two-bit [[BitRepresentation]]
   * in the sense that zeroing the high order bit of each pair and then removing every other bit
   * will map directly to this encoding.
   * E.g.: A = 0[0], G = 1[0]
   * C = 0[1] T = 1[1]
   */
  def charToBit(x: Char): Byte = (x: @switch) match {
    case 'r' | 'R' => 0 //A or G
    case 'y' | 'Y' => 1 //C or T
    case _ => throw new InvalidNucleotideException(x)
  }

  /** Convert a bit to a char, inverting the function above. */
  def bitToChar(x: Byte) = (x: @switch) match {
    case 0 => 'R'
    case 1 => 'Y'
  }

  //010101...01 64 bits
  private[this] final val twoToOneMask1: Long = 0x5555555555555555L
  private[this] final val twoToOneMask2: Long = 0x5555555555555555L << 1
  def twoBitToOneBit(x: Long): Long = {
    x & twoToOneMask1
  }

  def encode(x: NTSeq): NTBitArray = {
    ???
  }

  /** Convert a NTBitArray (using 1 bit per character) to a readable string,
   * using the RY alphabet.
   */
  def decode(x: NTBitArray): NTSeq = {
    val len = x.size / 2
    val sb = new StringBuilder(len)
    var i = 0
    while (i < len) {
      val char = x(i) //fetches two bits
      sb += bitToChar(((char & 2) >> 1).toByte)
      i += 1
      if (i < len) {
        sb += bitToChar((char & 1).toByte)
      }
      i += 1
    }
    sb.result()
  }
}
